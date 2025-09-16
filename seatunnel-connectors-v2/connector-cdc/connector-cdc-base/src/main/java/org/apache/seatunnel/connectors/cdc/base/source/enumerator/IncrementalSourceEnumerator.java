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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.LoggingUtils;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Incremental source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
public class IncrementalSourceEnumerator
        implements SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceEnumerator.class);

    private final SourceSplitEnumerator.Context<SourceSplitBase> context;
    private final SplitAssigner splitAssigner;

    /** using TreeSet to prefer assigning incremental split to task-0 for easier debug */
    private final TreeSet<Integer> readersAwaitingSplit;

    private volatile boolean running;

    public IncrementalSourceEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> context, SplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
        this.running = false;
    }

    @Override
    public void open() {
        LoggingUtils.logStart(LOG, "CDC Enumerator Initialization");
        splitAssigner.open();
        LoggingUtils.logEnd(LOG, "CDC Enumerator Initialization");
    }

    @Override
    public synchronized void run() throws Exception {

        LoggingUtils.logStart(LOG, "CDC Incremental Source Enumerator");
        LOG.info(
                "CDC configuration - Parallelism: {}, Registered readers: {}",
                context.currentParallelism(),
                context.registeredReaders());

        this.running = true;

        assignSplits();

        LoggingUtils.logEnd(LOG, "CDC Incremental Source Enumerator");
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        if (!context.registeredReaders().contains(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            LOG.warn("Reader {} is not registered, skipping split request", subtaskId);
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        if (running) {
            assignSplits();
        }
    }

    @Override
    public void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.info("Adding {} splits back to enumerator from reader: {}", splits.size(), subtaskId);

        splitAssigner.addSplits(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void registerReader(int subtaskId) {
        // do nothing
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.debug(
                "Handling CDC source event from subtask: {}, event type: {}",
                subtaskId,
                sourceEvent.getClass().getSimpleName());

        if (sourceEvent instanceof CompletedSnapshotSplitsReportEvent) {
            LOG.info(
                    "CDC enumerator receives completed split watermarks from subtask: {}",
                    subtaskId);
            CompletedSnapshotSplitsReportEvent reportEvent =
                    (CompletedSnapshotSplitsReportEvent) sourceEvent;
            List<SnapshotSplitWatermark> completedSplitWatermarks =
                    reportEvent.getCompletedSnapshotSplitWatermarks();
            synchronized (context) {
                splitAssigner.onCompletedSplits(completedSplitWatermarks);
            }

            // send acknowledge event
            CompletedSnapshotSplitsAckEvent ackEvent =
                    new CompletedSnapshotSplitsAckEvent(
                            completedSplitWatermarks.stream()
                                    .map(SnapshotSplitWatermark::getSplitId)
                                    .collect(Collectors.toList()));
            context.sendEventToSourceReader(subtaskId, ackEvent);

        } else if (sourceEvent instanceof CompletedSnapshotPhaseEvent) {
            LOG.info(
                    "CDC enumerator receives completed snapshot phase event from subtask: {}",
                    subtaskId);
            CompletedSnapshotPhaseEvent event = (CompletedSnapshotPhaseEvent) sourceEvent;
            if (splitAssigner instanceof HybridSplitAssigner) {
                ((HybridSplitAssigner) splitAssigner).completedSnapshotPhase(event.getTableIds());
                LOG.info(
                        "Cleaning SnapshotSplitAssigner#assignedSplits/splitCompletedOffsets to empty");
            }
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        LOG.debug("Creating checkpoint state for checkpoint: {}", checkpointId);
        PendingSplitsState state = splitAssigner.snapshotState(checkpointId);
        LOG.debug("Checkpoint state created successfully for checkpoint: {}", checkpointId);
        return state;
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        LOG.info("Checkpoint {} completed, notifying CDC split assigner", checkpointId);
        splitAssigner.notifyCheckpointComplete(checkpointId);

        // incremental split may be available after checkpoint complete
        LOG.debug("Checkpoint completed, attempting to assign incremental splits");
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        LOG.debug(
                "Starting CDC split assignment process, awaiting readers: {}",
                readersAwaitingSplit.size());

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().contains(nextAwaiting)) {
                LOG.warn(
                        "Reader {} is no longer registered, removing from awaiting list",
                        nextAwaiting);
                awaitingReader.remove();
                continue;
            }

            Optional<SourceSplitBase> split;
            synchronized (context) {
                split = splitAssigner.getNext();
            }
            if (split.isPresent()) {
                final SourceSplitBase sourceSplit = split.get();
                context.assignSplit(nextAwaiting, sourceSplit);
                awaitingReader.remove();

                LOG.info(
                        "Assigned CDC split {} to subtask {}", sourceSplit.splitId(), nextAwaiting);
                LOG.debug(
                        "Split details - Type: {}, Status: assigned",
                        sourceSplit.getClass().getSimpleName());

            } else {
                if (splitAssigner.waitingForCompletedSplits()) {
                    // there is no available splits by now, skip assigning
                    LOG.debug("No available splits, waiting for completed splits");
                    break;
                } else {
                    LOG.info(
                            "No more splits available, signaling no more splits to subtask: {}",
                            nextAwaiting);
                    context.signalNoMoreSplits(nextAwaiting);
                    awaitingReader.remove();
                }
            }
        }
    }
}
