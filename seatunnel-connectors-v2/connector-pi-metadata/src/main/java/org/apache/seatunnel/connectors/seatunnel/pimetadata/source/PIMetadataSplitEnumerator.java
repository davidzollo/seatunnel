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

package org.apache.seatunnel.connectors.seatunnel.pimetadata.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PIPathValidator;
import org.apache.seatunnel.connectors.seatunnel.pimetadata.split.PIMetadataSplit;
import org.apache.seatunnel.connectors.seatunnel.pimetadata.state.PIMetadataEnumeratorState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Slf4j
public class PIMetadataSplitEnumerator
        implements SourceSplitEnumerator<PIMetadataSplit, PIMetadataEnumeratorState> {

    private final Context<PIMetadataSplit> context;
    private final PIConfigHelper configHelper;
    private final Set<PIMetadataSplit> pendingSplits;
    private final Object stateLock = new Object();

    public PIMetadataSplitEnumerator(
            Context<PIMetadataSplit> context, PIConfigHelper configHelper) {
        this.context = context;
        this.configHelper = configHelper;
        this.pendingSplits = new HashSet<>();
        initializeSplits();
    }

    public PIMetadataSplitEnumerator(
            Context<PIMetadataSplit> context,
            PIConfigHelper configHelper,
            PIMetadataEnumeratorState state) {
        this.context = context;
        this.configHelper = configHelper;
        this.pendingSplits = new HashSet<>(state.getPendingSplits());
    }

    private void initializeSplits() {
        List<String> allPaths = configHelper.getPiPaths();

        // Use batch size optimized specifically for metadata, default 20 paths per split
        int batchSize = getOptimalBatchSize(allPaths);

        log.info(
                "Initializing PI metadata splits for {} paths with optimized batch size {} (optimized for large number of paths)",
                allPaths.size(),
                batchSize);

        // Validate and resolve paths
        PIPathValidator.validatePiPaths(allPaths);

        // Create splits by batching paths
        for (int i = 0; i < allPaths.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, allPaths.size());
            List<String> pathBatch = allPaths.subList(i, endIndex);

            String splitId = String.format("pi-metadata-split-%d", i / batchSize);
            PIMetadataSplit split =
                    new PIMetadataSplit(splitId, new ArrayList<>(pathBatch), i / batchSize);

            pendingSplits.add(split);
            log.debug("Created split {} with {} paths", splitId, pathBatch.size());
        }

        log.info("Created {} splits for PI metadata collection", pendingSplits.size());
    }

    @Override
    public void open() {
        log.info("PI Metadata Split Enumerator opened");
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        synchronized (stateLock) {
            assignPendingSplits();
        }

        log.info("No more splits to assign. Sending NoMoreSplitsEvent to readers: {}", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        log.info("PI Metadata Split Enumerator closed");
    }

    @Override
    public void addSplitsBack(List<PIMetadataSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            log.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);

            synchronized (stateLock) {
                pendingSplits.addAll(splits);

                if (context.registeredReaders().contains(subtaskId)) {
                    assignPendingSplitsToReader(subtaskId);
                } else {
                    log.warn("Reader {} not registered, splits added to pending queue", subtaskId);
                }
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        log.debug("Handling split request from subtask {}", subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to PI Metadata Split Enumerator", subtaskId);

        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignPendingSplitsToReader(subtaskId);
            }
        }
    }

    @Override
    public PIMetadataEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            log.debug("Creating snapshot for checkpoint {}", checkpointId);
            return new PIMetadataEnumeratorState(new ArrayList<>(pendingSplits), new ArrayList<>());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.info("Checkpoint {} completed", checkpointId);
    }

    /** Assign pending splits to registered readers */
    private void assignPendingSplits() {
        if (pendingSplits.isEmpty()) {
            log.info("No pending splits to assign");
            return;
        }

        Collection<Integer> readers = context.registeredReaders();
        if (readers.isEmpty()) {
            log.info("No registered readers available");
            return;
        }

        List<PIMetadataSplit> splitsToAssign = new ArrayList<>(pendingSplits);
        pendingSplits.clear();

        Integer[] readerArray = readers.toArray(new Integer[0]);
        for (int i = 0; i < splitsToAssign.size(); i++) {
            PIMetadataSplit split = splitsToAssign.get(i);
            int readerId = readerArray[i % readerArray.length];

            context.assignSplit(readerId, split);
        }

        log.info("Successfully assigned {} splits", splitsToAssign.size());
    }

    /**
     * Assign pending splits to a specific reader
     *
     * @param readerId
     */
    private void assignPendingSplitsToReader(int readerId) {
        if (pendingSplits.isEmpty()) {
            return;
        }

        Iterator<PIMetadataSplit> iterator = pendingSplits.iterator();
        if (iterator.hasNext()) {
            PIMetadataSplit split = iterator.next();
            iterator.remove();

            context.assignSplit(readerId, split);
            log.info("Assigned split {} to reader {}", split.splitId(), readerId);
        }
    }

    /**
     * Get batch size optimized for PI Metadata, considering GET request URL length limits and
     * performance optimization for large number of paths
     *
     * <p>Important: PI Web API metadata endpoint only supports GET requests, not POST. Therefore,
     * URL length must be strictly controlled to avoid exceeding server limits
     */
    private int getOptimalBatchSize(List<String> allPaths) {
        // Further reduce default batch size for GET request URL limits
        int defaultBatchSize = 10; // Safe batch size for GET requests

        if (allPaths.isEmpty()) {
            return defaultBatchSize;
        }

        // Estimate average path length
        int totalLength = allPaths.stream().mapToInt(String::length).sum();
        int avgPathLength = totalLength / allPaths.size();

        // Consider URL encoding length increase (about 50%, conservative estimate) and base URL
        // length
        int encodedAvgLength = (int) (avgPathLength * 1.5);
        int baseUrlLength =
                150; // Base URL length estimation (including /piwebapi/points/multiple? etc.)
        int maxUrlLength =
                4000; // Conservative URL length limit, considering various server configurations

        // Calculate theoretical maximum batch size
        int theoreticalMaxBatch =
                (maxUrlLength - baseUrlLength)
                        / (encodedAvgLength + 25); // +25 for parameter separators etc.

        // Take smaller value to ensure safety, minimum is 1
        int optimalBatchSize = Math.min(defaultBatchSize, Math.max(1, theoreticalMaxBatch));

        // Further safety check: if calculated batch size is too large, force limit
        if (optimalBatchSize > 15) {
            optimalBatchSize = 15; // Absolute safe upper limit for GET requests
        }

        log.info(
                "GET request URL optimization - Average path length: {}, Estimated encoded length: {}, Theoretical max batch: {}, Final batch size: {} (GET request limitation)",
                avgPathLength,
                encodedAvgLength,
                theoreticalMaxBatch,
                optimalBatchSize);

        return optimalBatchSize;
    }
}
