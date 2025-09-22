/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.pi.split;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PIPathValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PI CDC split enumerator - intelligent split scheduling core component
 *
 * <p>Responsible for intelligent split scheduling of large PI data sources and assigning them to
 * multiple Readers for parallel processing, optimized based on PI Web API technical limitations:
 *
 * <ul>
 *   <li>Real-time mode: 25 PI Paths/split (WebSocket URL length limit 8KB)
 *   <li>Support load balancing and checkpoint mechanism
 *   <li>Maximum support 160,000 PI Paths, 10,000 splits
 * </ul>
 *
 * <p>Core Design Principles:
 *
 * <ul>
 *   <li>Principle 1: Keep split size - max 25 PI Paths per split
 *   <li>Principle 2: Normal case single split, fault tolerance allows multiple splits - max 50 PI
 *       Paths per reader
 *   <li>Principle 3: Simplified recovery - directly use checkpoint splits without reorganization
 *   <li>Principle 4: Dynamic allocation during fault tolerance - round-robin distribution based on
 *       PI Path count
 * </ul>
 */
public class PICDCSplitEnumerator
        implements SourceSplitEnumerator<PICDCSplit, PICDCCheckpointState> {

    private static final Logger log = LoggerFactory.getLogger(PICDCSplitEnumerator.class);

    /**
     * Maximum number of WebIDs per split for CDC real-time mode - limited by WebSocket URL length
     * (about 8KB). This value is configurable via max_webids_per_split parameter.
     */
    private final int maxWebIDsPerSplit;

    /** SeaTunnel split enumerator context, used for interaction with engine */
    private final SourceSplitEnumerator.Context<PICDCSplit> context;

    /** List of PI Paths to be processed */
    private final List<String> piPaths;

    /** Map of Reader ID -> list of pending splits */
    private final Map<Integer, List<PICDCSplit>> pendingSplits;

    /** Thread synchronization lock, protecting shared state */
    private final Object stateLock = new Object();

    /** Whether to execute split enumeration (to prevent duplicate execution) */
    private boolean shouldEnumerate;

    /** Checkpoint state manager */
    private PICDCCheckpointState checkpointState;

    /** Create split enumerator from PI configuration */
    public PICDCSplitEnumerator(PIConfigHelper configHelper, Context<PICDCSplit> context) {
        this(configHelper, context, null);
    }

    /** Constructor from checkpoint for fault recovery */
    public PICDCSplitEnumerator(
            PIConfigHelper configHelper,
            Context<PICDCSplit> context,
            PICDCCheckpointState checkpointState) {
        this.context = context;

        this.pendingSplits = new HashMap<>();
        this.shouldEnumerate = true;
        this.checkpointState =
                checkpointState != null
                        ? checkpointState
                        : new PICDCCheckpointState(new ArrayList<>(), new ArrayList<>());

        // Read max PI Paths per split from configuration (default 25 for fault tolerance)
        this.maxWebIDsPerSplit = configHelper.getMaxWebIDsPerSplit();

        // Restore split state from checkpoint
        if (checkpointState != null) {
            restoreFromCheckpoint(checkpointState);
        }

        // Get PI Paths from configuration
        if (configHelper.getPiPaths() != null) {
            this.piPaths = new ArrayList<>(configHelper.getPiPaths());
        } else {
            this.piPaths = new ArrayList<>();
        }
    }

    /** Initialize enumerator and validate configuration */
    @Override
    public void open() {
        if (piPaths.isEmpty()) {
            log.warn("PI Path list is empty, cannot create split");
            return;
        }

        log.info("PI CDC split enumerator started - PI Path total: {}", piPaths.size());

        // Validate split configuration
        validateSplitConfiguration();
    }

    /** Register new Reader and assign pending splits */
    @Override
    public void registerReader(int subtaskId) {
        log.info("Register new Reader, subtaskId: {}", subtaskId);

        // Assign pending splits to new Reader
        synchronized (stateLock) {
            List<PICDCSplit> readerSplits = pendingSplits.get(subtaskId);
            if (readerSplits != null && !readerSplits.isEmpty()) {
                log.info(
                        "Assign {} pre-stored splits to new registered Reader-{}",
                        readerSplits.size(),
                        subtaskId);
                context.assignSplit(subtaskId, new ArrayList<>(readerSplits));
                readerSplits.clear();

                // Clean up empty split list
                if (readerSplits.isEmpty()) {
                    pendingSplits.remove(subtaskId);
                }

                // Immediately send no more splits signal, allowing Reader to start processing
                context.signalNoMoreSplits(subtaskId);
                log.info(
                        "Sent no more splits signal to Reader-{}, Reader can start processing data",
                        subtaskId);
            } else {
                log.info("Reader-{} registered, but no splits assigned", subtaskId);
                // Send signal to avoid Reader waiting indefinitely
                context.signalNoMoreSplits(subtaskId);
            }
        }
    }

    /** Create and assign splits to registered Readers */
    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();

        if (shouldEnumerate && !piPaths.isEmpty()) {
            log.info("Start creating CDC splits, registered Reader count: {}", readers.size());

            // Create splits based on read mode
            List<PICDCSplit> splits = validatePiPathsCountAndCreateSplits();

            synchronized (stateLock) {
                if (readers.isEmpty()) {
                    // Save splits to pending list for later assignment
                    log.info(
                            "No registered Reader, save {} splits to pending list, waiting for Reader registration",
                            splits.size());

                    // Pre-allocate splits to Reader slots
                    for (int i = 0; i < splits.size(); i++) {
                        int readerId = i % Math.max(1, context.currentParallelism());
                        pendingSplits
                                .computeIfAbsent(readerId, k -> new ArrayList<>())
                                .add(splits.get(i));
                    }

                    log.info(
                            "Splits have been pre-allocated to {} Reader slots, waiting for Reader registration",
                            Math.max(1, context.currentParallelism()));
                } else {
                    // Distribute splits to registered Readers
                    distributeSplitsToReaders(splits, readers);
                }

                shouldEnumerate = false;
            }
        }

        // Send no more splits signal to registered Readers
        if (!readers.isEmpty()) {
            readers.forEach(context::signalNoMoreSplits);
        }
    }

    /** Handle split recovery when Reader fails or restarts */
    @Override
    public void addSplitsBack(List<PICDCSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }

        log.info("Reader {} add splits back, recovering {} CDC splits", subtaskId, splits.size());

        synchronized (stateLock) {
            // Create new splits with current timestamp for recovery
            List<PICDCSplit> recoveredSplits = new ArrayList<>();
            for (PICDCSplit split : splits) {
                // Create new split with current time
                PICDCSplit recoveredSplit =
                        new PICDCSplit(
                                split.splitId(), split.getPiPaths(), System.currentTimeMillis());

                recoveredSplits.add(recoveredSplit);

                log.debug(
                        "Reset split {} start time to current moment for CDC recovery",
                        split.splitId());
            }

            // Redistribute recovered splits using round-robin (Principle 4)
            Set<Integer> availableReaders = context.registeredReaders();
            if (!availableReaders.isEmpty()) {
                List<Integer> readerList = new ArrayList<>(availableReaders);
                for (int i = 0; i < recoveredSplits.size(); i++) {
                    int targetReader = readerList.get(i % readerList.size());
                    pendingSplits
                            .computeIfAbsent(targetReader, k -> new ArrayList<>())
                            .add(recoveredSplits.get(i));
                }

                // Validate that no reader exceeds WebID limit after redistribution (Principle 2)
                validateReaderWebIDLimits();

                log.info(
                        "Redistributed {} recovered splits to {} available readers using round-robin",
                        recoveredSplits.size(),
                        availableReaders.size());
            } else {
                // No readers available, keep splits pending
                pendingSplits
                        .computeIfAbsent(subtaskId, k -> new ArrayList<>())
                        .addAll(recoveredSplits);
                log.warn("No available readers, keeping {} splits pending", recoveredSplits.size());
            }
        }
    }

    /** Get total number of unassigned splits */
    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }
    }

    /** Handle Reader split request and assign available splits */
    @Override
    public void handleSplitRequest(int subtaskId) {
        log.debug("Handle split request, subtaskId: {}", subtaskId);

        synchronized (stateLock) {
            List<PICDCSplit> readerSplits = pendingSplits.get(subtaskId);
            if (readerSplits != null && !readerSplits.isEmpty()) {
                // Assign split to requesting Reader
                PICDCSplit split = readerSplits.remove(0);
                context.assignSplit(subtaskId, Collections.singletonList(split));

                // Clean up empty split list
                if (readerSplits.isEmpty()) {
                    pendingSplits.remove(subtaskId);
                }
            }
        }
    }

    /** Create checkpoint state snapshot for fault recovery */
    @Override
    public PICDCCheckpointState snapshotState(long checkpointId) throws Exception {
        log.debug("Save split enumerator state, checkpointId: {}", checkpointId);

        synchronized (stateLock) {
            // Save complete split state for fault recovery
            List<PICDCSplit> remainingSplits = new ArrayList<>();
            for (List<PICDCSplit> splits : pendingSplits.values()) {
                remainingSplits.addAll(splits);
            }

            // Assigned splits (keep empty list in CDC mode)
            List<PICDCSplit> assignedSplits = new ArrayList<>();

            PICDCCheckpointState snapshot =
                    new PICDCCheckpointState(remainingSplits, assignedSplits);
            snapshot.setCheckpointId(checkpointId);

            log.debug(
                    "Checkpoint state snapshot created - remaining splits: {}, assigned splits: {}",
                    remainingSplits.size(),
                    assignedSplits.size());

            return snapshot;
        }
    }

    /** Handle checkpoint completion notification */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Checkpoint completion notification, checkpointId: {}", checkpointId);

        synchronized (stateLock) {
            // Only update the updated checkpoint ID
            if (checkpointState.getCheckpointId() <= checkpointId) {
                checkpointState.setCheckpointId(checkpointId);
            }
        }
    }

    /** Close enumerator and cleanup resources */
    @Override
    public void close() throws IOException {
        log.info("PI CDC split enumerator closed");
        synchronized (stateLock) {
            pendingSplits.clear();
        }
    }

    /** Validate PI Path count and create splits */
    private List<PICDCSplit> validatePiPathsCountAndCreateSplits() {
        List<PICDCSplit> splits = new ArrayList<>();
        int maxWebIDsPerSplit = this.maxWebIDsPerSplit;

        // Calculate expected split count
        int expectedSplitCount = (piPaths.size() + maxWebIDsPerSplit - 1) / maxWebIDsPerSplit;
        int parallelism = Math.max(1, context.currentParallelism());

        // Validate parallelism is sufficient to handle all splits
        if (expectedSplitCount > parallelism) {
            int maxAllowedPiPaths = parallelism * maxWebIDsPerSplit;
            String errorMsg =
                    String.format(
                            "Insufficient parallelism for PICDC connector. "
                                    + "Total PI Path count: %d, max per split: %d, expected splits: %d, current parallelism: %d. "
                                    + "To avoid WebSocket URL length limit, each Reader can only handle one split with max %d PI Paths. "
                                    + "Please reduce the PI Path count to %d or fewer.",
                            piPaths.size(),
                            maxWebIDsPerSplit,
                            expectedSplitCount,
                            parallelism,
                            maxWebIDsPerSplit,
                            maxAllowedPiPaths);

            log.error(errorMsg);
            throw new PIConnectorException(PIErrorCode.CONFIG_VALIDATION_FAILED, errorMsg);
        }

        log.info(
                "PICDC parallelism validation passed - PI Path count: {}, expected splits: {}, parallelism: {}",
                piPaths.size(),
                expectedSplitCount,
                parallelism);

        // Split PI Path list by split size
        for (int i = 0; i < piPaths.size(); i += maxWebIDsPerSplit) {
            int endIndex = Math.min(i + maxWebIDsPerSplit, piPaths.size());
            List<String> splitPiPaths = piPaths.subList(i, endIndex);

            // Generate unique split ID
            String splitId = String.format("cdc-split-%d", i / maxWebIDsPerSplit);

            // Only use PI Paths
            List<String> splitPiPathList = new ArrayList<>(splitPiPaths);

            // Create split with current timestamp
            PICDCSplit split =
                    new PICDCSplit(
                            splitId,
                            splitPiPathList,
                            System.currentTimeMillis() // Start from current time
                            );
            splits.add(split);

            log.debug("Create CDC split: {}, PI Path count: {}", splitId, splitPiPaths.size());
        }

        log.info(
                "Split creation completed, total split count: {}, total PI Path count: {}",
                splits.size(),
                piPaths.size());
        return splits;
    }

    /** Distribute splits to readers using round-robin algorithm */
    private void distributeSplitsToReaders(List<PICDCSplit> splits, Set<Integer> readers) {
        if (readers.isEmpty()) {
            log.warn("No registered Reader, splits will remain pending");
            return;
        }

        List<Integer> readerList = new ArrayList<>(readers);
        log.info(
                "Start allocating splits to Readers, Reader count: {}, split count: {}",
                readers.size(),
                splits.size());

        // Use round-robin allocation
        for (int i = 0; i < splits.size(); i++) {
            int readerId = readerList.get(i % readerList.size());
            pendingSplits.computeIfAbsent(readerId, k -> new ArrayList<>()).add(splits.get(i));
        }

        // Allocate splits to each Reader (one split at a time to avoid WebSocket URL length limit)
        for (Integer readerId : readers) {
            List<PICDCSplit> readerSplits = pendingSplits.get(readerId);
            if (readerSplits != null && !readerSplits.isEmpty()) {
                // Debug: Log reader split allocation details
                int totalPaths =
                        readerSplits.stream()
                                .mapToInt(
                                        split ->
                                                (split.getPiPaths() != null
                                                        ? split.getPiPaths().size()
                                                        : 0))
                                .sum();
                int originalSplitCount = readerSplits.size();
                log.info(
                        "Reader {} will receive {} splits with total {} paths",
                        readerId,
                        originalSplitCount,
                        totalPaths);

                // Assign splits one by one to avoid WebID accumulation
                for (PICDCSplit split : new ArrayList<>(readerSplits)) {
                    context.assignSplit(readerId, Collections.singletonList(split));
                    readerSplits.remove(split);
                    log.debug("Assigned split {} to Reader-{}", split.splitId(), readerId);
                }
                log.info(
                        "Allocated {} splits to Reader-{} (one by one)",
                        originalSplitCount,
                        readerId);
            }
        }

        log.info("Split allocation completed");
    }

    /** Validate split configuration */
    private void validateSplitConfiguration() {
        int totalPiPaths = piPaths.size();
        int maxWebIDsPerSplit = this.maxWebIDsPerSplit;
        int expectedSplits = (totalPiPaths + maxWebIDsPerSplit - 1) / maxWebIDsPerSplit;

        // Validate and resolve paths
        PIPathValidator.validatePiPaths(piPaths);

        // Validate PI Path count limit
        if (totalPiPaths > 160000) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PI Path total count %d exceeds system limit 160,000", totalPiPaths));
        }

        // Validate split count limit
        if (expectedSplits > 10000) {
            log.warn(
                    "Expected split count {} exceeds warning line 10,000, may affect performance",
                    expectedSplits);
        }

        log.info(
                "Split configuration validation passed - PI Path count: {}, max per split: {}, expected splits: {}",
                totalPiPaths,
                maxWebIDsPerSplit,
                expectedSplits);
    }

    /** Validate that no reader exceeds WebID limit (Principle 2: max 50 WebIDs per reader) */
    private void validateReaderWebIDLimits() {
        for (Map.Entry<Integer, List<PICDCSplit>> entry : pendingSplits.entrySet()) {
            int readerId = entry.getKey();
            List<PICDCSplit> readerSplits = entry.getValue();

            int totalPiPaths = 0;
            for (PICDCSplit split : readerSplits) {
                if (split.getPiPaths() != null) {
                    totalPiPaths += split.getPiPaths().size();
                }
            }

            if (totalPiPaths > 50) { // WebSocket URL limit
                throw new IllegalStateException(
                        String.format(
                                "Reader %d assigned %d PI Paths (from %d splits), exceeds limit 50. "
                                        + "Please increase parallelism or reduce PI Path count.",
                                readerId, totalPiPaths, readerSplits.size()));
            }

            log.info(
                    "Reader {} assigned {} splits with total {} PI Paths",
                    readerId,
                    readerSplits.size(),
                    totalPiPaths);
        }
    }

    /** Restore split state from checkpoint */
    private void restoreFromCheckpoint(PICDCCheckpointState checkpointState) {
        try {
            List<PICDCSplit> remainingSplits = checkpointState.getRemainingSplits();
            if (!remainingSplits.isEmpty()) {
                log.info("Restore {} splits from checkpoint", remainingSplits.size());

                // Debug: Log checkpoint split details
                for (int i = 0; i < remainingSplits.size(); i++) {
                    PICDCSplit split = remainingSplits.get(i);
                    int piPathCount = (split.getPiPaths() != null ? split.getPiPaths().size() : 0);
                    log.info("Checkpoint Split {}: {} PI paths", i, piPathCount);
                }

                int currentParallelism = Math.max(1, context.currentParallelism());

                // Directly use checkpoint splits without reorganization (Principle 3)
                // Allow multiple splits per reader in case of parallelism mismatch (Principle 2)
                for (int i = 0; i < remainingSplits.size(); i++) {
                    int readerId = i % currentParallelism;
                    pendingSplits
                            .computeIfAbsent(readerId, k -> new ArrayList<>())
                            .add(remainingSplits.get(i));
                }

                // Validate that no reader exceeds WebID limit (Principle 2: max 50 WebIDs per
                // reader)
                validateReaderWebIDLimits();

                // No need to re-enumerate if splits exist
                this.shouldEnumerate = false;
                log.info("Checkpoint restoration completed, will use recovered splits");
            } else {
                log.info("No remaining splits in checkpoint, will re-create splits");
                this.shouldEnumerate = true;
            }
        } catch (Exception e) {
            log.error("Failed to restore split state from checkpoint, will re-create splits", e);
            pendingSplits.clear();
            this.shouldEnumerate = true;
        }
    }
}
