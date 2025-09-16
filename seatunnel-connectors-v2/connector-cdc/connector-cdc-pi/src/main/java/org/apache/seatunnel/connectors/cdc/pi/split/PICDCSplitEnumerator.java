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
 *   <li>Real-time mode: 50 WebIDs/split (WebSocket URL length limit 8KB)
 *   <li>Support load balancing and checkpoint mechanism
 *   <li>Maximum support 160,000 WebIDs, 10,000 splits
 * </ul>
 */
public class PICDCSplitEnumerator
        implements SourceSplitEnumerator<PICDCSplit, PICDCCheckpointState> {

    private static final Logger log = LoggerFactory.getLogger(PICDCSplitEnumerator.class);

    /**
     * Maximum number of WebIDs per split for CDC real-time mode - limited by WebSocket URL length
     * (about 8KB)
     */
    private static final int MAX_WEBIDS_PER_SPLIT = 50;

    /** SeaTunnel split enumerator context, used for interaction with engine */
    private final SourceSplitEnumerator.Context<PICDCSplit> context;

    /** List of WebIDs to be processed (use PI Path first) */
    private final List<String> webIds;

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

        // Restore split state from checkpoint
        if (checkpointState != null) {
            restoreFromCheckpoint(checkpointState);
        }

        // Get WebID or PI Path from configuration
        if (configHelper.getPiPaths() != null) {
            this.webIds = new ArrayList<>(configHelper.getPiPaths());
        } else if (configHelper.getWebIds() != null) {
            this.webIds = new ArrayList<>(configHelper.getWebIds());
        } else {
            this.webIds = new ArrayList<>();
        }
    }

    /** Initialize enumerator and validate configuration */
    @Override
    public void open() {
        if (webIds.isEmpty()) {
            log.warn("WebID list is empty, cannot create split");
            return;
        }

        log.info("PI CDC split enumerator started - WebID total: {}", webIds.size());

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

        if (shouldEnumerate && !webIds.isEmpty()) {
            log.info("Start creating CDC splits, registered Reader count: {}", readers.size());

            // Create splits based on read mode
            List<PICDCSplit> splits = createSplitsByMode();

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
                                split.splitId(),
                                split.getPiPaths(),
                                split.getWebIds(),
                                System.currentTimeMillis());
                recoveredSplits.add(recoveredSplit);
                log.debug(
                        "Reset split {} start time to current moment for CDC recovery",
                        split.splitId());
            }

            pendingSplits
                    .computeIfAbsent(subtaskId, k -> new ArrayList<>())
                    .addAll(recoveredSplits);

            // Reassign if reader is available
            if (context.registeredReaders().contains(subtaskId)) {
                context.assignSplit(subtaskId, new ArrayList<>(recoveredSplits));
                pendingSplits.get(subtaskId).removeAll(recoveredSplits);
                context.signalNoMoreSplits(subtaskId);

                log.info(
                        "Immediately reassigned {} splits to recovered reader {}",
                        recoveredSplits.size(),
                        subtaskId);
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

    /** Create splits based on read mode */
    private List<PICDCSplit> createSplitsByMode() {
        List<PICDCSplit> splits = new ArrayList<>();
        int maxWebIDsPerSplit = MAX_WEBIDS_PER_SPLIT;

        // Validate total capacity based on parallelism
        int parallelism = Math.max(1, context.currentParallelism());
        int maxTotalWebIds = parallelism * maxWebIDsPerSplit;

        if (webIds.size() > maxTotalWebIds) {
            String errorMsg =
                    String.format(
                            "Total WebID/PI Path count (%d) exceeds system capacity (%d). "
                                    + "Current parallelism: %d, max WebIDs per split: %d. "
                                    + "Please reduce PI Path count to %d or increase parallelism to %d.",
                            webIds.size(),
                            maxTotalWebIds,
                            parallelism,
                            maxWebIDsPerSplit,
                            maxTotalWebIds,
                            (int) Math.ceil((double) webIds.size() / maxWebIDsPerSplit));

            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        log.info(
                "Create CDC splits - parallelism: {}, max WebIDs per split: {}, total capacity: {}, actual WebIDs: {}",
                parallelism,
                maxWebIDsPerSplit,
                maxTotalWebIds,
                webIds.size());

        // Split WebID list by split size
        for (int i = 0; i < webIds.size(); i += maxWebIDsPerSplit) {
            int endIndex = Math.min(i + maxWebIDsPerSplit, webIds.size());
            List<String> splitWebIDs = webIds.subList(i, endIndex);

            // Generate unique split ID
            String splitId = String.format("cdc-split-%d", i / maxWebIDsPerSplit);

            // Determine whether to use PI Path or WebID
            List<String> piPaths = new ArrayList<>();
            List<String> webIdList = new ArrayList<>();

            // Check if splitWebIDs is PI Path or WebID
            if (isUsingPiPaths()) {
                piPaths.addAll(splitWebIDs);
            } else {
                webIdList.addAll(splitWebIDs);
            }

            // Create split with current timestamp
            PICDCSplit split =
                    new PICDCSplit(
                            splitId,
                            piPaths,
                            webIdList,
                            System.currentTimeMillis() // Start from current time
                            );
            splits.add(split);

            log.debug("Create CDC split: {}, WebID count: {}", splitId, splitWebIDs.size());
        }

        log.info(
                "Split creation completed, total split count: {}, total WebID count: {}",
                splits.size(),
                webIds.size());
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

        // Allocate splits to each Reader
        for (Integer readerId : readers) {
            List<PICDCSplit> readerSplits = pendingSplits.get(readerId);
            if (readerSplits != null && !readerSplits.isEmpty()) {
                log.info("Allocate {} splits to Reader-{}", readerSplits.size(), readerId);
                context.assignSplit(readerId, new ArrayList<>(readerSplits));
                readerSplits.clear();
            }
        }

        log.info("Split allocation completed");
    }

    /** Validate split configuration */
    private void validateSplitConfiguration() {
        int totalWebIDs = webIds.size();
        int maxWebIDsPerSplit = MAX_WEBIDS_PER_SPLIT;
        int expectedSplits = (totalWebIDs + maxWebIDsPerSplit - 1) / maxWebIDsPerSplit;

        // Validate and resolve paths
        PIPathValidator.validatePiPaths(webIds);

        // Validate WebID count limit
        if (totalWebIDs > 160000) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "WebID total count %d exceeds system limit 160,000", totalWebIDs));
        }

        // Validate split count limit
        if (expectedSplits > 10000) {
            log.warn(
                    "Expected split count {} exceeds warning line 10,000, may affect performance",
                    expectedSplits);
        }

        log.info(
                "Split configuration validation passed - WebID count: {}, max per split: {}, expected splits: {}",
                totalWebIDs,
                maxWebIDsPerSplit,
                expectedSplits);
    }

    /** Restore split state from checkpoint */
    private void restoreFromCheckpoint(PICDCCheckpointState checkpointState) {
        try {
            List<PICDCSplit> remainingSplits = checkpointState.getRemainingSplits();
            if (!remainingSplits.isEmpty()) {
                log.info("Restore {} splits from checkpoint", remainingSplits.size());

                // Re-allocate recovered splits to pending list
                for (int i = 0; i < remainingSplits.size(); i++) {
                    int readerId = i % Math.max(1, context.currentParallelism());
                    pendingSplits
                            .computeIfAbsent(readerId, k -> new ArrayList<>())
                            .add(remainingSplits.get(i));
                }

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

    /** Check if using PI Path format or WebID format */
    private boolean isUsingPiPaths() {
        // Check format of webIds
        if (webIds.isEmpty()) {
            return false;
        }

        // Check first ID format
        String firstId = webIds.get(0);

        // PI Path contains path separator or dot
        boolean hasPiPathCharacters =
                firstId.contains("\\") || firstId.contains("/") || firstId.contains(".");

        // WebID is long string without path separators
        boolean looksLikeWebId =
                firstId.length() > 40
                        && !firstId.contains("\\")
                        && !firstId.contains("/")
                        && !firstId.contains(".");

        // Check PI Path first
        if (hasPiPathCharacters) {
            return true;
        }

        // Return false for WebID format
        if (looksLikeWebId) {
            return false;
        }

        // Default to PI Path
        return true;
    }
}
