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

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PI CDC split enumerator for round-robin split allocation. Each split contains a list of PI Paths
 * to track in CDC PIRealtimeReader.
 */
@Slf4j
public class PICDCSplitEnumerator
        implements SourceSplitEnumerator<PICDCSplit, PICDCCheckpointState> {

    /**
     * Maximum number of WebIDs per split - limited by WebSocket URL length (about 8KB). This value
     * is configurable via max_webids_per_split parameter.
     */
    private final int maxWebIDsPerSplit;

    // SeaTunnel split enumerator context, used for interaction with engine
    private final SourceSplitEnumerator.Context<PICDCSplit> context;

    // List of PI Paths to be processed
    private final List<String> piPaths;

    // Pending splits map: splitId -> split
    private final Map<String, PICDCSplit> pendingSplitsMap = new HashMap<>();

    // Assigned splits map: splitId -> split (CDC long-running tracking to prevent duplicate
    // assignment)
    private final Map<String, PICDCSplit> assignedSplitsMap = new HashMap<>();

    // Optional: split ownership mapping for auditing (splitId -> readerId)
    private final Map<String, Integer> splitOwnerMap = new HashMap<>();

    // Thread synchronization lock, protecting shared state
    private final Object stateLock = new Object();

    // Whether splits should be enumerated (to prevent duplicate enumeration)
    private volatile boolean shouldEnumerated = true;

    // Checkpoint state manager (only records checkpointId)
    private final PICDCCheckpointState checkpointState;

    /** Create split enumerator from PI configuration. */
    public PICDCSplitEnumerator(PIConfigHelper configHelper, Context<PICDCSplit> context) {
        this(configHelper, context, null);
    }

    /**
     * Constructor from checkpoint for fault recovery.
     *
     * @param configHelper PI connector configuration helper
     * @param context enumerator context
     * @param checkpointState checkpoint state to restore from
     */
    public PICDCSplitEnumerator(
            PIConfigHelper configHelper,
            Context<PICDCSplit> context,
            PICDCCheckpointState checkpointState) {
        this.context = context;

        this.shouldEnumerated = true;
        this.checkpointState =
                checkpointState != null
                        ? checkpointState
                        : new PICDCCheckpointState(new ArrayList<>(), new ArrayList<>());

        this.maxWebIDsPerSplit = configHelper.getMaxWebIDsPerSplit();

        // Restore splits from checkpoint if available
        if (checkpointState != null) {
            int restoredPending = 0;
            int restoredAssigned = 0;
            // restore assigned first
            List<PICDCSplit> assigned = checkpointState.getAssignedSplits();
            if (assigned != null) {
                for (PICDCSplit s : assigned) {
                    if (s != null) {
                        assignedSplitsMap.put(s.splitId(), s);
                        restoredAssigned++;
                    }
                }
            }
            // then restore remaining, skip ids already in assigned
            List<PICDCSplit> remaining = checkpointState.getRemainingSplits();
            if (remaining != null) {
                for (PICDCSplit s : remaining) {
                    if (s != null && !assignedSplitsMap.containsKey(s.splitId())) {
                        pendingSplitsMap.put(s.splitId(), s);
                        restoredPending++;
                    }
                }
            }
            if (restoredPending > 0 || restoredAssigned > 0) {
                shouldEnumerated = false;
                // Requeue assigned splits after recovery to ensure they will be dispatched again
                if (!assignedSplitsMap.isEmpty()) {
                    int requeued = 0;
                    for (PICDCSplit s : assignedSplitsMap.values()) {
                        if (s != null) {
                            pendingSplitsMap.put(s.splitId(), s);
                            requeued++;
                        }
                    }
                    assignedSplitsMap.clear();
                    log.info("Requeued {} assigned splits after restore", requeued);
                }

                log.info(
                        "Checkpoint recovery detected (checkpointId: {}), restored pending={}, assigned={}",
                        checkpointState.getCheckpointId(),
                        restoredPending,
                        restoredAssigned);
            }
        }

        // Get PI Paths from configuration
        if (configHelper.getPiPaths() != null) {
            this.piPaths = new ArrayList<>(configHelper.getPiPaths());
        } else {
            this.piPaths = new ArrayList<>();
        }
    }

    /** Initialize enumerator and validate configuration. */
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

    /**
     * Register new Reader and assign pending splits.
     *
     * @param subtaskId
     */
    @Override
    public void registerReader(int subtaskId) {
        log.info("Reader-{} registered", subtaskId);

        synchronized (stateLock) {
            // Try to assign one initial split to the newly registered Reader
            PICDCSplit split = getOneSplitFromPendingMap();
            if (split != null) {
                // Track assignment to avoid duplicate re-assignment
                assignedSplitsMap.put(split.splitId(), split);
                splitOwnerMap.put(split.splitId(), subtaskId);
                context.assignSplit(subtaskId, Collections.singletonList(split));

                // Reminder: Do NOT send signalNoMoreSplits here - let Reader request more splits as
                // needed,this enables multi-split processing per Reader
                log.info(
                        "Assigned initial split {} to Reader-{}, remaining pending: {}, assigned: {}",
                        split.splitId(),
                        subtaskId,
                        pendingSplitsMap.size(),
                        assignedSplitsMap.size());
            } else {
                log.info(
                        "No pending splits for Reader-{}, will wait for split requests", subtaskId);
            }
        }
    }

    /** Initialize splits and distribute to registered Readers. */
    @Override
    public void run() {
        if (shouldEnumerated && !piPaths.isEmpty()) {
            synchronized (stateLock) {
                if (shouldEnumerated) {
                    log.info("Enumerating PI CDC splits, PI paths count: {}", piPaths.size());

                    // Create all splits and put into pending map (deduplicated by splitId)
                    List<PICDCSplit> splits = generateSplitsFromPiPaths();
                    for (PICDCSplit s : splits) {
                        pendingSplitsMap.put(s.splitId(), s);
                    }
                    shouldEnumerated = false;

                    log.info("Enumerated {} splits into pending map", splits.size());

                    // Immediately distribute pending splits to registered readers
                    distributeSplitsToReaders();
                }
            }
        }
    }

    /**
     * Handle splits returned by Reader due to failure/pause/resume. Simply add returned splits back
     * to the global queue for reassignment.
     *
     * @param splits returned splits
     * @param subtaskId the returning reader id
     */
    @Override
    public void addSplitsBack(List<PICDCSplit> splits, int subtaskId) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            int added = 0;
            for (PICDCSplit s : splits) {
                if (s == null) {
                    continue;
                }
                // remove from assigned if present
                assignedSplitsMap.remove(s.splitId());
                // remove owner if present and warn on mismatch
                Integer owner = splitOwnerMap.remove(s.splitId());
                if (owner != null && owner.intValue() != subtaskId) {
                    log.warn(
                            "Split {} returned by Reader-{} but owned by Reader-{}",
                            s.splitId(),
                            subtaskId,
                            owner);
                }
                // put back to pending (dedup by map key)
                pendingSplitsMap.put(s.splitId(), s);
                added++;
            }
            log.info(
                    "Reader-{} returned {} splits, added back to pending splits map. pending={}, assigned={}",
                    subtaskId,
                    added,
                    pendingSplitsMap.size(),
                    assignedSplitsMap.size());
        }
    }

    /**
     * Get total number of unassigned splits.
     *
     * @return number of unassigned splits
     */
    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplitsMap.size();
    }

    /**
     * Handle Reader's request for additional splits. Assign available splits from global queue on
     * demand. This enables dynamic multi-split processing per Reader.
     *
     * @param subtaskId the requesting reader id
     */
    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            // Validate subtaskId to prevent NullPointerException in context operations
            Set<Integer> registeredReaders = context.registeredReaders();
            if (!registeredReaders.contains(subtaskId)) {
                log.debug(
                        "Received split request from subtask {} before registration (readers: {}), will retry after registration",
                        subtaskId,
                        registeredReaders);
                // Instead of rejecting, defer the request - the reader will retry
                return;
            }

            PICDCSplit split = getOneSplitFromPendingMap();
            if (split != null) {
                assignedSplitsMap.put(split.splitId(), split);
                splitOwnerMap.put(split.splitId(), subtaskId);
                context.assignSplit(subtaskId, Collections.singletonList(split));
                log.info(
                        "Assigned requested split {} to Reader-{}, remaining pending: {}, assigned: {}",
                        split.splitId(),
                        subtaskId,
                        pendingSplitsMap.size(),
                        assignedSplitsMap.size());
            } else {
                // Only signal no more splits when enumeration is complete and queue is empty
                if (!shouldEnumerated) {
                    context.signalNoMoreSplits(subtaskId);
                    log.info(
                            "No more splits pending for Reader-{}, signaled completion", subtaskId);
                } else {
                    log.debug(
                            "Enumeration not yet complete, Reader-{} will retry split request",
                            subtaskId);
                }
            }
        }
    }

    /**
     * Save enumerator state: pending + assigned splits for CDC recovery.
     *
     * @param checkpointId checkpoint identifier
     * @return snapshot state containing pending and assigned splits
     */
    @Override
    public PICDCCheckpointState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            log.debug("Save split enumerator state, checkpointId: {}", checkpointId);
            List<PICDCSplit> remainingSplits = new ArrayList<>(pendingSplitsMap.values());
            List<PICDCSplit> assignedSplits = new ArrayList<>(assignedSplitsMap.values());
            PICDCCheckpointState snapshot =
                    new PICDCCheckpointState(remainingSplits, assignedSplits);
            snapshot.setCheckpointId(checkpointId);
            log.debug(
                    "Checkpoint state snapshot created - remaining: {}, assigned: {}",
                    remainingSplits.size(),
                    assignedSplits.size());
            return snapshot;
        }
    }

    /**
     * PI CDC enumerator does not need to take any action on checkpoint completion, but we update
     * the checkpoint ID in the state for tracking purposes.
     *
     * @param checkpointId checkpoint identifier
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        log.debug("Checkpoint completion notification, checkpointId: {}", checkpointId);

        synchronized (stateLock) {
            // Only update the updated checkpoint ID
            if (checkpointState.getCheckpointId() <= checkpointId) {
                checkpointState.setCheckpointId(checkpointId);
            }
        }
    }

    /** Close enumerator and cleanup resources. */
    @Override
    public void close() {
        synchronized (stateLock) {
            log.info(
                    "PI CDC split enumerator closed, clearing pending={}, assigned={}",
                    pendingSplitsMap.size(),
                    assignedSplitsMap.size());
            pendingSplitsMap.clear();
            assignedSplitsMap.clear();
            splitOwnerMap.clear();
        }
    }

    /**
     * Generate splits from PI Paths, each split contains up to maxWebIDsPerSplit PI Paths.
     * Validates that expected split count does not exceed parallelism to ensure 1:1 Reader-Split
     * allocation.
     *
     * @return list of generated splits
     */
    private List<PICDCSplit> generateSplitsFromPiPaths() {
        List<PICDCSplit> splits = new ArrayList<>();
        int maxWebIDsPerSplit = this.maxWebIDsPerSplit;

        // Calculate expected split count
        int expectedSplitCount = (piPaths.size() + maxWebIDsPerSplit - 1) / maxWebIDsPerSplit;
        int parallelism = Math.max(1, context.currentParallelism());

        // Multi-split mode: Allow split count to exceed parallelism for fault tolerance
        // When a Reader fails, other Readers can pick up its splits for automatic failover
        if (expectedSplitCount > parallelism) {
            log.warn(
                    "Split count {} exceeds parallelism {}, enabling multi-split mode. "
                            + "Normal operation: 1:1 Reader-Split ratio. "
                            + "During failover: Remaining Readers will handle multiple splits. "
                            + "Recommendation: Consider increasing parallelism to {} for optimal 1:1 allocation.",
                    expectedSplitCount,
                    parallelism,
                    expectedSplitCount);
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

            // Create split (initial checkpoint time is 0, Reader will advance)
            PICDCSplit split = new PICDCSplit(splitId, splitPiPathList);
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
     * Distribute pending splits from global queue to registered readers (1:1 allocation) Assign one
     * split per reader in round-robin.
     */
    private void distributeSplitsToReaders() {
        Set<Integer> readers = context.registeredReaders();
        if (readers.isEmpty()) {
            log.info("No registered readers, splits will remain pending");
            return;
        }

        int distributedCount = 0;
        for (Integer readerId : readers) {
            PICDCSplit split = getOneSplitFromPendingMap();
            if (split != null) {
                assignedSplitsMap.put(split.splitId(), split);
                splitOwnerMap.put(split.splitId(), readerId);
                context.assignSplit(readerId, Collections.singletonList(split));
                distributedCount++;
                log.info("Distributed split {} to Reader-{}", split.splitId(), readerId);
            }
        }

        if (distributedCount > 0) {
            log.info(
                    "Distributed {} splits to {} readers, remaining pending: {}, assigned: {}",
                    distributedCount,
                    readers.size(),
                    pendingSplitsMap.size(),
                    assignedSplitsMap.size());
        }

        // Do NOT send signalNoMoreSplits here - let Readers request more splits as needed
        // This maintains the standard 1:1 Reader-Split allocation pattern
    }

    /**
     * Get and remove one split from pending map (arbitrary order).
     *
     * @return a split or null when no pending split
     */
    private PICDCSplit getOneSplitFromPendingMap() {
        if (pendingSplitsMap.isEmpty()) {
            return null;
        }
        // get first entry
        String firstKey = null;
        for (Map.Entry<String, PICDCSplit> e : pendingSplitsMap.entrySet()) {
            firstKey = e.getKey();
            break;
        }
        if (firstKey == null) {
            return null;
        }
        return pendingSplitsMap.remove(firstKey);
    }

    /** Validate split configuration. */
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
