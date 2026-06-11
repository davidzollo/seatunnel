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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.tracing.MDCTracer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiTableSinkWriter
        implements SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState>,
                SupportSchemaEvolutionSinkWriter {

    private final Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext;
    private final Map<String, Optional<Integer>> sinkPrimaryKeys = new HashMap<>();

    @Getter
    private final List<ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>>
            sinkWritersWithIndex;

    private final List<MultiTableWriterRunnable> runnable = new ArrayList<>();
    private final Random random = new Random();
    private final List<BlockingQueue<MultiTableWriterRunnable.QueueElement>> blockingQueues =
            new ArrayList<>();
    private final ExecutorService executorService;
    private MultiTableResourceManager resourceManager;
    private volatile boolean submitted = false;

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext) {
        this.sinkWriters = sinkWriters;
        this.sinkWritersContext = sinkWritersContext;
        AtomicInteger cnt = new AtomicInteger(0);
        executorService =
                MDCTracer.tracing(
                        Executors.newFixedThreadPool(
                                // we use it in `MultiTableWriterRunnable` and `prepare commit
                                // task`, so it
                                // should be double.
                                queueSize * 2,
                                runnable -> {
                                    Thread thread = new Thread(runnable);
                                    thread.setDaemon(true);
                                    thread.setName(
                                            "st-multi-table-sink-writer"
                                                    + "-"
                                                    + cnt.incrementAndGet());
                                    return thread;
                                }));
        sinkWritersWithIndex = new ArrayList<>();
        for (int i = 0; i < queueSize; i++) {
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue =
                    new LinkedBlockingQueue<>(1024);
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
            ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierMap =
                    new ConcurrentHashMap<>();
            int queueIndex = i;
            sinkWriters.entrySet().stream()
                    .filter(entry -> entry.getKey().getIndex() % queueSize == queueIndex)
                    .forEach(
                            entry -> {
                                tableIdWriterMap.put(
                                        entry.getKey().getTableIdentifier(), entry.getValue());
                                sinkIdentifierMap.put(entry.getKey(), entry.getValue());
                            });

            sinkWritersWithIndex.add(sinkIdentifierMap);
            blockingQueues.add(queue);
            MultiTableWriterRunnable r = new MultiTableWriterRunnable(tableIdWriterMap, queue);
            runnable.add(r);
        }
        log.info("init multi table sink writer, queue size: {}", queueSize);
        initResourceManager(queueSize);
    }

    private void initResourceManager(int queueSize) {
        for (SinkIdentifier tableIdentifier : sinkWriters.keySet()) {
            SinkWriter<SeaTunnelRow, ?, ?> sink = sinkWriters.get(tableIdentifier);
            if (sink instanceof MultiTableTtlWriter) {
                SinkWriter<SeaTunnelRow, ?, ?> ttlWriter = ((MultiTableTtlWriter) sink).create();
                resourceManager =
                        ((SupportMultiTableSinkWriter<?>) ttlWriter)
                                .initMultiTableResourceManager(
                                        sinkWritersWithIndex.size(), queueSize);
            } else {
                resourceManager =
                        ((SupportMultiTableSinkWriter<?>) sink)
                                .initMultiTableResourceManager(sinkWriters.size(), queueSize);
            }
            break;
        }

        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writerMap =
                    sinkWritersWithIndex.get(i);
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> entry :
                    writerMap.entrySet()) {
                if (entry.getValue() instanceof MultiTableTtlWriter) {
                    MultiTableTtlWriter multiTableTtlWriter =
                            (MultiTableTtlWriter) entry.getValue();
                    multiTableTtlWriter.setMultiTableResourceManager(resourceManager, i);
                } else {
                    SupportMultiTableSinkWriter<?> sink =
                            ((SupportMultiTableSinkWriter<?>) entry.getValue());
                    sink.setMultiTableResourceManager(resourceManager, i);
                    sinkPrimaryKeys.put(entry.getKey().getTableIdentifier(), sink.primaryKey());
                }
            }
        }
    }

    /**
     * Surfaces the first queue-worker failure through the checked IOException contract used by the
     * schema-change, write and snapshot APIs.
     */
    private void subSinkErrorCheck() throws IOException {
        IOException failure = currentSubSinkFailure();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Converts the first observed queue-worker failure into IOException so coordinator methods keep
     * one consistent failure channel.
     */
    private IOException currentSubSinkFailure() {
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            Throwable throwable = writerRunnable.getThrowable();
            if (throwable != null) {
                if (throwable instanceof IOException) {
                    return (IOException) throwable;
                }
                return new IOException(
                        String.format(
                                "table %s sink throw error", writerRunnable.getCurrentTableId()),
                        throwable);
            }
        }
        return null;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        subSinkErrorCheck();
        if (!hasSourceMatchedWriter(event)) {
            return;
        }
        ensureQueueWorkersSubmitted();
        subSinkErrorCheck();
        enqueueSchemaChangeBarrier(event);
    }

    /**
     * Keeps the schema-change path on the legacy source-table contract so unrelated table events
     * can still return immediately without waking queue workers.
     */
    private boolean hasSourceMatchedWriter(SchemaChangeEvent event) {
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (sinkWriterEntry
                        .getKey()
                        .getTableIdentifier()
                        .equals(event.tablePath().getFullName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Collects every sub-writer that must observe this schema change event. We first route by
     * source-table identifier, then fan the same event out to sibling sub-writers that advertise
     * the same physical sink table identifier.
     */
    private List<SchemaChangeDispatchTarget> collectSchemaChangeDispatchTargets(
            SchemaChangeEvent event) {
        Set<String> primarySharedSinkIds = new HashSet<>();
        Set<SinkIdentifier> primaryDispatchedKeys = new HashSet<>();
        List<SchemaChangeDispatchTarget> dispatchTargets = new ArrayList<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (sinkWriterEntry
                        .getKey()
                        .getTableIdentifier()
                        .equals(event.tablePath().getFullName())) {
                    dispatchTargets.add(
                            new SchemaChangeDispatchTarget(
                                    sinkWriterEntry.getKey(),
                                    sinkWriterEntry.getValue(),
                                    "source-match"));
                    primaryDispatchedKeys.add(sinkWriterEntry.getKey());
                    extractPhysicalSinkIdentifier(sinkWriterEntry.getValue())
                            .ifPresent(primarySharedSinkIds::add);
                }
            }
        }

        // Step 2: when the matched sub-writer(s) advertised a physical sink table identifier,
        // broadcast the event to every sibling sub-writer that targets the same physical table.
        // Multi-table sinks that resolve a sink-table template per upstream source (e.g. JDBC
        // sink with cdc${table_name}) end up with several sub-writers sharing one physical
        // destination; without this fan-out the sibling sub-writers keep their in-memory output
        // format pointing at the old schema and crash on the next commit with errors such as
        // "Unknown column 'col4'". Dialect-level columnExists guards (JdbcDialect.applySchemaChange
        // for ADD/DROP/CHANGE) make the duplicate ALTER attempts a safe no-op (issue #4252).
        if (primarySharedSinkIds.isEmpty()) {
            return dispatchTargets;
        }
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (primaryDispatchedKeys.contains(sinkWriterEntry.getKey())) {
                    continue;
                }
                Optional<String> siblingPhysicalSinkId =
                        extractPhysicalSinkIdentifier(sinkWriterEntry.getValue());
                if (siblingPhysicalSinkId.isPresent()
                        && primarySharedSinkIds.contains(siblingPhysicalSinkId.get())) {
                    dispatchTargets.add(
                            new SchemaChangeDispatchTarget(
                                    sinkWriterEntry.getKey(),
                                    sinkWriterEntry.getValue(),
                                    "shared-physical-sink " + siblingPhysicalSinkId.get()));
                }
            }
        }
        return dispatchTargets;
    }

    /**
     * Routes schema changes through the same queue workers as data rows. Every queue must first
     * drain older rows, then all workers stop at the shared barrier, and only then does one worker
     * mutate the shared sink schema. That ordering closes the reviewer-reported hole where queued
     * old-schema rows could be consumed by a freshly rebuilt writer.
     */
    private void enqueueSchemaChangeBarrier(SchemaChangeEvent event) throws IOException {
        SchemaChangeBarrier schemaChangeBarrier =
                new SchemaChangeBarrier(
                        event, runnable.size(), this::dispatchSchemaChangeToTargets);
        try {
            for (BlockingQueue<MultiTableWriterRunnable.QueueElement> blockingQueue :
                    blockingQueues) {
                offerQueueElement(
                        blockingQueue,
                        MultiTableWriterRunnable.schemaChangeRequest(schemaChangeBarrier));
            }
        } catch (IOException e) {
            schemaChangeBarrier.fail(e);
            throw e;
        }
        schemaChangeBarrier.awaitCompletion();
    }

    /**
     * Runs the final schema-change fan-out only after every queue worker has already drained older
     * rows and reached the shared barrier.
     */
    private void dispatchSchemaChangeToTargets(SchemaChangeEvent event) throws IOException {
        List<SchemaChangeDispatchTarget> dispatchTargets =
                collectSchemaChangeDispatchTargets(event);
        for (SchemaChangeDispatchTarget dispatchTarget : dispatchTargets) {
            applySchemaChangeToTarget(event, dispatchTarget);
        }
    }

    /**
     * Applies the schema change to one sub-writer while every queue worker is already waiting on
     * the same schema-change barrier.
     */
    private void applySchemaChangeToTarget(
            SchemaChangeEvent event, SchemaChangeDispatchTarget dispatchTarget) throws IOException {
        log.info(
                "Start apply schema change for table {} sub-writer {} ({})",
                dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                dispatchTarget.getSinkIdentifier().getIndex(),
                dispatchTarget.getReason());
        if (dispatchTarget.getWriter() instanceof SupportSchemaEvolutionSinkWriter) {
            ((SupportSchemaEvolutionSinkWriter) dispatchTarget.getWriter())
                    .applySchemaChange(event);
        } else {
            // TODO remove deprecated method
            dispatchTarget.getWriter().applySchemaChange(event);
        }
        log.info(
                "Finish apply schema change for table {} sub-writer {} ({})",
                dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                dispatchTarget.getSinkIdentifier().getIndex(),
                dispatchTarget.getReason());
    }

    /**
     * Reads the optional physical sink identifier from one sub-writer and normalizes buggy null
     * Optional implementations back to {@link Optional#empty()}.
     */
    private Optional<String> extractPhysicalSinkIdentifier(SinkWriter<SeaTunnelRow, ?, ?> writer) {
        if (writer instanceof SupportSchemaEvolutionSinkWriter) {
            // Defensive normalization keeps buggy connectors that return null Optional values from
            // crashing the low-frequency but high-impact schema-change path.
            Optional<String> physicalSinkIdentifier =
                    ((SupportSchemaEvolutionSinkWriter) writer).getPhysicalSinkTableIdentifier();
            return physicalSinkIdentifier == null ? Optional.empty() : physicalSinkIdentifier;
        }
        return Optional.empty();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        ensureQueueWorkersSubmitted();
        subSinkErrorCheck();
        Optional<Integer> primaryKey =
                sinkPrimaryKeys.computeIfAbsent(
                        element.getTableId(),
                        v -> {
                            Optional<Integer> pk = Optional.empty();
                            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterMap =
                                    sinkWritersWithIndex.get(0);
                            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>
                                    sinkWriterEntry : sinkWriterMap.entrySet()) {
                                if (sinkWriterEntry.getValue() instanceof MultiTableTtlWriter) {
                                    return Optional.empty();
                                }
                                if (sinkWriterEntry
                                        .getKey()
                                        .getTableIdentifier()
                                        .equals(element.getTableId())) {
                                    SupportMultiTableSinkWriter<?> supportMultiTableSinkWriter =
                                            (SupportMultiTableSinkWriter<?>)
                                                    sinkWriterEntry.getValue();
                                    pk = supportMultiTableSinkWriter.primaryKey();
                                    break;
                                }
                            }
                            return pk;
                        });
        if (sinkPrimaryKeys.size() == 1 || !primaryKey.isPresent()) {
            int index = random.nextInt(blockingQueues.size());
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue = blockingQueues.get(index);
            offerQueueElement(queue, MultiTableWriterRunnable.rowRequest(element));
        } else {
            Object object = element.getField(primaryKey.get());
            int index = 0;
            if (object != null) {
                index = Math.abs(object.hashCode()) % blockingQueues.size();
            }
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue = blockingQueues.get(index);
            offerQueueElement(queue, MultiTableWriterRunnable.rowRequest(element));
        }
    }

    /**
     * Starts the queue workers exactly once before the first ordered request enters the queues.
     * Schema changes must share the same bootstrap path as row writes so the first DDL cannot
     * bypass the in-band barrier while another thread is still bringing the workers online.
     */
    private synchronized void ensureQueueWorkersSubmitted() {
        if (submitted) {
            return;
        }
        runnable.forEach(executorService::submit);
        submitted = true;
    }

    /** Keeps queue insertion logic consistent for both row writes and schema-change barriers. */
    private void offerQueueElement(
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue,
            MultiTableWriterRunnable.QueueElement queueElement)
            throws IOException {
        try {
            while (!queue.offer(queueElement, 500, TimeUnit.MILLISECONDS)) {
                subSinkErrorCheck();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public List<MultiTableState> snapshotState(long checkpointId) throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        List<MultiTableState> multiTableStates = new ArrayList<>();
        MultiTableState multiTableState = new MultiTableState(new HashMap<>());
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                synchronized (runnable.get(i)) {
                    try {
                        List<?> states = sinkWriterEntry.getValue().snapshotState(checkpointId);
                        multiTableState.getStates().put(sinkWriterEntry.getKey(), states);
                    } catch (Exception e) {
                        String message =
                                String.format(
                                        "table %s snapshotState throw an error",
                                        sinkWriterEntry.getKey().getTableIdentifier());
                        log.error(message, e);
                        throw new RuntimeException(message, e);
                    }
                }
            }
        }
        multiTableStates.add(multiTableState);
        return multiTableStates;
    }

    @Override
    public Optional<MultiTableCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<MultiTableCommitInfo> prepareCommit(long checkpointId) throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        MultiTableCommitInfo multiTableCommitInfo =
                new MultiTableCommitInfo(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            int subWriterIndex = i;
            futures.add(
                    executorService.submit(
                            () -> {
                                synchronized (runnable.get(subWriterIndex)) {
                                    for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>
                                            sinkWriterEntry :
                                                    sinkWritersWithIndex
                                                            .get(subWriterIndex)
                                                            .entrySet()) {
                                        Optional<?> commit;
                                        try {
                                            commit =
                                                    sinkWriterEntry
                                                            .getValue()
                                                            .prepareCommit(checkpointId);
                                        } catch (Exception e) {
                                            String message =
                                                    String.format(
                                                            "table %s prepareCommit throw an error",
                                                            sinkWriterEntry
                                                                    .getKey()
                                                                    .getTableIdentifier());
                                            log.error(message, e);
                                            throw new RuntimeException(message, e);
                                        }
                                        commit.ifPresent(
                                                o ->
                                                        multiTableCommitInfo
                                                                .getCommitInfo()
                                                                .put(sinkWriterEntry.getKey(), o));
                                    }
                                }
                            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (multiTableCommitInfo.getCommitInfo().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(multiTableCommitInfo);
    }

    @Override
    public void abortPrepare() {
        Throwable firstE = null;
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE = e;
        }
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                for (SinkWriter<SeaTunnelRow, ?, ?> sinkWriter :
                        sinkWritersWithIndex.get(i).values()) {
                    try {
                        sinkWriter.abortPrepare();
                    } catch (Throwable e) {
                        if (firstE == null) {
                            firstE = e;
                        }
                        log.error("abortPrepare error", e);
                    }
                }
            }
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
        }
    }

    @Override
    public void close() throws IOException {
        // The variables used in lambda expressions should be final or valid final, so they are
        // modified to arrays
        final Throwable[] firstE = {null};
        try {
            checkQueueRemain();
        } catch (Throwable e) {
            firstE[0] = e;
        }
        executorService.shutdownNow();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierSinkWriterMap =
                        sinkWritersWithIndex.get(i);
                sinkIdentifierSinkWriterMap.forEach(
                        (identifier, sinkWriter) -> {
                            try {
                                sinkWriter.close();
                                sinkWritersContext
                                        .get(identifier)
                                        .getEventListener()
                                        .onEvent(new WriterCloseEvent());
                            } catch (Throwable e) {
                                if (firstE[0] == null) {
                                    firstE[0] = e;
                                }
                                log.error("close error", e);
                            }
                        });
            }
        }
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        if (firstE[0] != null) {
            throw new RuntimeException(firstE[0]);
        }
    }

    /**
     * Waits until every queue drains its backlog or surfaces the worker failure that stopped
     * progress.
     */
    private void checkQueueRemain() throws IOException {
        try {
            for (BlockingQueue<MultiTableWriterRunnable.QueueElement> blockingQueue :
                    blockingQueues) {
                while (!blockingQueue.isEmpty()) {
                    Thread.sleep(100);
                    subSinkErrorCheck();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
