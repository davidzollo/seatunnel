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
 * PI CDC split enumerator - Round-robin and reliable split scheduling core component
 *
 * <p>Responsible for Round-robin split scheduling of large PI data sources and assigning them to
 * multiple Readers for parallel processing, optimized based on PI Web API technical limitations:
 *
 * <ul>
 *   <li>Real-time mode: 25 PI Paths/split (WebSocket URL length limit 8KB)
 *   <li>Support round-robin load balancing and checkpoint mechanism
 *   <li>Maximum support 160,000 PI Paths, 10,000 splits
 * </ul>
 *
 * <p>Core Design Principles:
 *
 * <ul>
 *   <li>Principle 1: Keep split size - max 25 PI Paths per split (WebSocket URL limit)
 *   <li>Principle 2: Round-robin distribution ensures balanced allocation
 *   <li>Principle 3: Complete reallocation strategy - recreate all splits on failure/resume
 *   <li>Principle 4: Minimal state tracking - only track pending allocations, no Reader states
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

        // With complete reallocation strategy, ignore checkpoint and always recreate
        if (checkpointState != null) {
            log.info(
                    "Using recovery strategy: ignore checkpoint and recreate from original PI Paths");
            // Complete state cleanup for clean recovery
            pendingSplits.clear();
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
                // Assign all splits to the reader
                for (PICDCSplit split : new ArrayList<>(readerSplits)) {
                    context.assignSplit(subtaskId, Collections.singletonList(split));
                }
                // Remove assigned splits
                pendingSplits.remove(subtaskId);

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
                    // Pre-allocate splits using round-robin for future Reader registration
                    log.info(
                            "No registered Reader, pre-allocate {} splits using round-robin",
                            splits.size());

                    int currentParallelism = Math.max(1, context.currentParallelism());
                    for (int i = 0; i < splits.size(); i++) {
                        int readerId = i % currentParallelism;
                        pendingSplits
                                .computeIfAbsent(readerId, k -> new ArrayList<>())
                                .add(splits.get(i));
                    }

                    log.info("Splits pre-allocated successfully, waiting for Reader registration");
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

    /**
     * Handle splits returned by Reader due to various reasons. Called by SeaTunnel framework in
     * three scenarios: 1. Reader failure/crash - Reader returns unfinished splits 2. Job pause -
     * Reader returns current processing splits for checkpoint 3. Job resume - Framework returns
     * splits from previous checkpoint
     *
     * <p>PI CDC uses "complete reallocation strategy": - Ignores returned splits (they become
     * invalid after WebSocket disconnection) - Clears all allocation states and recreates splits
     * from original PI Paths - Reassigns all splits to healthy readers starting from current time
     */
    @Override
    public void addSplitsBack(List<PICDCSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }

        log.info(
                "Reader {} returned {} splits (failure/pause/resume), applying complete reallocation strategy",
                subtaskId,
                splits.size());

        boolean needReallocation = false;

        synchronized (stateLock) {
            // Prevent duplicate reallocation if another thread is already processing
            // This can happen when multiple readers fail simultaneously
            if (!shouldEnumerate) {
                // Clear all existing allocation states to start fresh
                pendingSplits.clear();
                shouldEnumerate = true;
                needReallocation = true;

                log.info(
                        "Cleared all allocation states, will recreate and redistribute all splits from original PI Paths");
            } else {
                log.info(
                        "Split reallocation already in progress, ignoring duplicate request from Reader {}",
                        subtaskId);
            }
        }

        // Execute reallocation outside synchronized block to prevent potential deadlock
        // The run() method will create new reader threads and assign splits, which may acquire
        // other locks
        if (needReallocation) {
            try {
                run();
            } catch (Exception e) {
                log.error("Failed to execute split reallocation", e);
                // Reset state on failure to allow manual retry or next automatic trigger
                synchronized (stateLock) {
                    shouldEnumerate = false;
                    pendingSplits.clear(); // Clean up inconsistent state
                }
                throw new RuntimeException(
                        "Critical: Split reallocation failed, manual intervention may be required",
                        e);
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

    /**
     * Handle Reader's request for additional splits.
     *
     * <p>PI CDC uses push-based split allocation model (like Kafka connector): - All splits are
     * pre-allocated during reader registration - Readers establish WebSocket connections and wait
     * for data - No dynamic split request is needed or supported
     *
     * <p>If this method is called, it indicates a potential issue in Reader implementation.
     */
    @Override
    public void handleSplitRequest(int subtaskId) {
        // Do nothing because PI CDC source uses push-based split allocation
        log.warn(
                "PI CDC Reader-{} requested additional splits, but CDC mode does not support dynamic split allocation. "
                        + "All splits should have been assigned during reader registration. This shouldn't happen.",
                subtaskId);
    }

    /**
     * Create checkpoint state snapshot.
     *
     * <p>Note: PI CDC uses "complete reallocation strategy", so saved checkpoint state will be
     * ignored during recovery. However, we still implement this method to: 1. Comply with SeaTunnel
     * framework requirements 2. Provide debugging information about current state 3. Support
     * potential future optimizations
     */
    @Override
    public PICDCCheckpointState snapshotState(long checkpointId) throws Exception {
        log.debug("Save split enumerator state, checkpointId: {}", checkpointId);

        synchronized (stateLock) {
            // Save current pending splits (will be ignored during recovery due to complete
            // reallocation)
            List<PICDCSplit> remainingSplits = new ArrayList<>();
            for (List<PICDCSplit> splitList : pendingSplits.values()) {
                remainingSplits.addAll(splitList);
            }

            // Assigned splits (keep empty list as splits are immediately assigned in CDC mode)
            List<PICDCSplit> assignedSplits = new ArrayList<>();

            PICDCCheckpointState snapshot =
                    new PICDCCheckpointState(remainingSplits, assignedSplits);
            snapshot.setCheckpointId(checkpointId);

            log.debug(
                    "Checkpoint state snapshot created - remaining splits: {}, assigned splits: {} "
                            + "(Note: will be ignored during recovery due to complete reallocation strategy)",
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

    /**
     * Distribute splits to readers using simple round-robin allocation. Direct assignment without
     * intermediate storage.
     */
    private void distributeSplitsToReaders(List<PICDCSplit> splits, Set<Integer> readers) {
        synchronized (stateLock) {
            if (readers.isEmpty()) {
                log.info("No registered Reader, splits will remain pending");
                return;
            }

            List<Integer> readerList = new ArrayList<>(readers);
            log.info(
                    "Start round-robin allocation: {} splits to {} readers",
                    splits.size(),
                    readers.size());

            // Direct round-robin assignment to readers
            for (int i = 0; i < splits.size(); i++) {
                int readerId = readerList.get(i % readerList.size());
                PICDCSplit split = splits.get(i);

                try {
                    context.assignSplit(readerId, Collections.singletonList(split));
                    log.info("Assigned split {} to Reader-{}", split.splitId(), readerId);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign split {} to Reader-{}", split.splitId(), readerId, e);
                    // Add to pending for retry
                    pendingSplits.computeIfAbsent(readerId, k -> new ArrayList<>()).add(split);
                }
            }

            log.info("Round-robin allocation completed");
        }
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
}
