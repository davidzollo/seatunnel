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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for issue #4252: the multi-table sink coordinator must broadcast schema-
 * change events to every sub-writer that targets the same physical sink table. Without this fan-
 * out, a sink-table template such as {@code cdc${table_name}} that collapses several upstream
 * tables into one physical destination would let the first sub-writer mutate the database schema
 * while sibling sub-writers keep their in-memory output format pointing at the old schema and crash
 * on the next commit with errors such as {@code Unknown column 'col4'} - exactly the runtime
 * failure captured live on the WhaleStudio cluster while reproducing this bug.
 */
public class MultiTableSinkWriterSchemaChangeBroadcastTest {

    private static final String PHYSICAL_SINK_SHARED = "WHRTest.cdcusers1";
    private static final String PHYSICAL_SINK_OTHER = "WHRTest.cdcmysql_all_type_has_key_cdc";

    /**
     * The classic #4252 case: source {@code 9-1.users1} runs {@code DROP COLUMN col4}, the engine
     * emits the schema-change event against the {@code 9-1.users1} source path, and only the
     * source-matched sub-writer is reached by the legacy router. With the broadcast fix the
     * coordinator also fans the event out to {@code lyc_test.users1}'s sub-writer because both
     * resolve to the same physical destination table {@code WHRTest.cdcusers1}.
     */
    @Test
    void schemaChangeForOneSourceFansOutToSiblingsSharingThePhysicalSink() throws IOException {
        RecordingSinkWriter sinkForA = new RecordingSinkWriter("9-1.users1", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkForB =
                new RecordingSinkWriter("lyc_test.users1", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkForC =
                new RecordingSinkWriter("lyc_test.mysql_all_type_has_key_cdc", PHYSICAL_SINK_OTHER);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("9-1.users1", 0), sinkForA);
        writers.put(SinkIdentifier.of("lyc_test.users1", 0), sinkForB);
        writers.put(SinkIdentifier.of("lyc_test.mysql_all_type_has_key_cdc", 0), sinkForC);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        SchemaChangeEvent event = new TestSchemaChangeEvent(TablePath.of("9-1", null, "users1"));

        coordinator.applySchemaChange(event);

        assertEquals(
                1,
                sinkForA.invocationCount.get(),
                "source-matched sub-writer must receive the event exactly once");
        assertEquals(
                1,
                sinkForB.invocationCount.get(),
                "sibling sub-writer sharing the same physical sink must receive the event");
        assertEquals(
                0,
                sinkForC.invocationCount.get(),
                "unrelated sub-writer with a different physical sink must NOT receive the event");
    }

    /**
     * When no sub-writer shares the matched physical sink the broadcast is a no-op. Single-table
     * jobs and template-with-database scenarios keep the legacy strict routing intact.
     */
    @Test
    void singlePhysicalSinkDoesNotProduceAnyBroadcast() throws IOException {
        RecordingSinkWriter sinkA = new RecordingSinkWriter("dbA.users", "sinkA.cdcusersA");
        RecordingSinkWriter sinkB = new RecordingSinkWriter("dbB.users", "sinkB.cdcusersB");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        SchemaChangeEvent event = new TestSchemaChangeEvent(TablePath.of("dbA", null, "users"));

        coordinator.applySchemaChange(event);

        assertEquals(1, sinkA.invocationCount.get());
        assertEquals(0, sinkB.invocationCount.get(), "distinct physical sinks must stay isolated");
    }

    /**
     * Legacy sub-writers that do not implement {@link SupportSchemaEvolutionSinkWriter} (or report
     * an empty physical-sink identifier) keep the legacy strict per-source routing - broadcasting
     * never reaches them because the coordinator cannot tell whether they share a physical sink.
     */
    @Test
    void writersWithoutPhysicalSinkIdentifierStayOnLegacyRouting() throws IOException {
        RecordingSinkWriter sinkA = new RecordingSinkWriter("dbA.users", PHYSICAL_SINK_SHARED);
        LegacySinkWriter legacySibling = new LegacySinkWriter();

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), legacySibling);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkA.invocationCount.get());
        assertEquals(
                0,
                legacySibling.invocationCount.get(),
                "legacy writers without physical-sink id must not be reached by broadcast");
    }

    /**
     * If the same physical sink is reported by three sub-writers (e.g. db1/db2/db3 same-name tables
     * collapsing into one sink), the event must reach all three - the source-matched one directly
     * and the other two by broadcast.
     */
    @Test
    void schemaChangeReachesAllSiblingsWhenThreeSourcesShareOnePhysicalSink() throws IOException {
        RecordingSinkWriter a = new RecordingSinkWriter("db1.users", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter b = new RecordingSinkWriter("db2.users", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter c = new RecordingSinkWriter("db3.users", PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("db1.users", 0), a);
        writers.put(SinkIdentifier.of("db2.users", 0), b);
        writers.put(SinkIdentifier.of("db3.users", 0), c);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("db2", null, "users")));

        assertTrue(a.invocationCount.get() == 1, "non-matched sibling 1 receives broadcast");
        assertEquals(1, b.invocationCount.get(), "source-matched writer receives event once");
        assertTrue(c.invocationCount.get() == 1, "non-matched sibling 2 receives broadcast");
    }

    /**
     * The concurrency hole reported on the PR: queue-0 and queue-1 share one physical sink, but
     * queue-1 is still inside its write critical section when the schema change arrives. The
     * coordinator must freeze both runnables first, otherwise queue-0 mutates the sink schema while
     * queue-1 keeps writing rows with the old in-memory output format.
     */
    @Test
    void schemaChangeWaitsUntilSiblingQueueLeavesWriteCriticalSection() throws Exception {
        RecordingSinkWriter sinkA = new RecordingSinkWriter("dbA.users", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkB = new RecordingSinkWriter("dbB.users", PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 1), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 2, buildContextMap(writers));
        startQueueWorkers(coordinator);
        MultiTableWriterRunnable queueOneRunnable = getRunnable(coordinator, 1);

        CountDownLatch siblingWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseSiblingWrite = new CountDownLatch(1);
        Thread siblingWriteThread =
                new Thread(
                        () -> {
                            synchronized (queueOneRunnable) {
                                siblingWriteEntered.countDown();
                                awaitLatch(releaseSiblingWrite);
                            }
                        });
        siblingWriteThread.start();
        assertTrue(
                siblingWriteEntered.await(5, TimeUnit.SECONDS),
                "test setup must hold queue-1 runnable before schema change");

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        Thread schemaChangeThread =
                new Thread(
                        () -> {
                            schemaChangeSubmitted.countDown();
                            try {
                                coordinator.applySchemaChange(
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users")));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        schemaChangeThread.start();
        assertTrue(
                schemaChangeSubmitted.await(5, TimeUnit.SECONDS),
                "schema change thread must be running");

        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(
                0,
                sinkA.invocationCount.get(),
                "queue-0 must not apply schema change before queue-1 leaves the write critical section");
        assertEquals(
                0,
                sinkB.invocationCount.get(),
                "queue-1 sibling stays blocked until the shared lock scope is released");

        releaseSiblingWrite.countDown();
        siblingWriteThread.join(TimeUnit.SECONDS.toMillis(5));
        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(siblingWriteThread.isAlive(), "sibling write thread must exit cleanly");
        assertFalse(schemaChangeThread.isAlive(), "schema change thread must complete");
        assertEquals(1, sinkA.invocationCount.get());
        assertEquals(1, sinkB.invocationCount.get());
    }

    /**
     * Shared-sink discovery must not lazily prepare a TTL sibling before all queue runnables are
     * frozen. Otherwise schema-change routing would create that sibling's inner writer outside the
     * same synchronization path used by normal writes and reintroduce a double-prepare race.
     */
    @Test
    void lazyTtlSiblingProbeWaitsUntilSiblingQueueIsFrozen() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        RecordingSeaTunnelSink sinkA =
                new RecordingSeaTunnelSink("dbA.users", PHYSICAL_SINK_SHARED);
        RecordingSeaTunnelSink sinkB =
                new RecordingSeaTunnelSink("dbB.users", PHYSICAL_SINK_SHARED);
        MultiTableTtlWriter ttlA =
                new MultiTableTtlWriter(
                        writers, "dbA.users", 0, 1, sinkA, new TestSinkWriterContext(), 60);
        MultiTableTtlWriter ttlB =
                new MultiTableTtlWriter(
                        writers, "dbB.users", 1, 1, sinkB, new TestSinkWriterContext(), 60);
        writers.put(SinkIdentifier.of("dbA.users", 0), ttlA);
        writers.put(SinkIdentifier.of("dbB.users", 1), ttlB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 2, buildContextMap(writers));
        startQueueWorkers(coordinator);
        MultiTableWriterRunnable queueOneRunnable = getRunnable(coordinator, 1);

        assertEquals(1, sinkA.createCount.get());
        assertEquals(0, sinkB.createCount.get());

        CountDownLatch siblingQueueHeld = new CountDownLatch(1);
        CountDownLatch releaseSiblingQueue = new CountDownLatch(1);
        Thread siblingQueueThread =
                new Thread(
                        () -> {
                            synchronized (queueOneRunnable) {
                                siblingQueueHeld.countDown();
                                awaitLatch(releaseSiblingQueue);
                            }
                        });
        siblingQueueThread.start();
        assertTrue(siblingQueueHeld.await(5, TimeUnit.SECONDS));

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        Thread schemaChangeThread =
                new Thread(
                        () -> {
                            schemaChangeSubmitted.countDown();
                            try {
                                coordinator.applySchemaChange(
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users")));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        schemaChangeThread.start();
        assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));

        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(
                0,
                sinkB.createCount.get(),
                "lazy TTL sibling must not be prepared before queue-1 is frozen");

        releaseSiblingQueue.countDown();
        siblingQueueThread.join(TimeUnit.SECONDS.toMillis(5));
        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(siblingQueueThread.isAlive());
        assertFalse(schemaChangeThread.isAlive());
        assertEquals(1, sinkB.createCount.get(), "schema-change dispatch prepares sibling once");
        assertEquals(1, sinkB.lastCreatedWriter.invocationCount.get());
    }

    /**
     * A lazy TTL sibling that has never prepared its inner writer must still participate in the
     * shared-sink broadcast; otherwise the first schema change would mutate the physical table
     * while that sibling keeps no in-memory schema update at all.
     */
    @Test
    void lazyTtlSiblingStillReceivesBroadcastWhenPhysicalSinkIsShared() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        RecordingSeaTunnelSink sinkA =
                new RecordingSeaTunnelSink("dbA.users", PHYSICAL_SINK_SHARED);
        RecordingSeaTunnelSink sinkB =
                new RecordingSeaTunnelSink("dbB.users", PHYSICAL_SINK_SHARED);
        MultiTableTtlWriter ttlA =
                new MultiTableTtlWriter(
                        writers, "dbA.users", 0, 1, sinkA, new TestSinkWriterContext(), 60);
        MultiTableTtlWriter ttlB =
                new MultiTableTtlWriter(
                        writers, "dbB.users", 0, 1, sinkB, new TestSinkWriterContext(), 60);
        writers.put(SinkIdentifier.of("dbA.users", 0), ttlA);
        writers.put(SinkIdentifier.of("dbB.users", 0), ttlB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        assertEquals(
                1, sinkA.createCount.get(), "resource-manager init prepares the first TTL writer");
        assertEquals(
                0,
                sinkB.createCount.get(),
                "the sibling TTL writer stays lazy until shared-sink routing probes it");

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkA.lastCreatedWriter.invocationCount.get());
        assertEquals(
                1,
                sinkB.createCount.get(),
                "shared-sink discovery must initialize the lazy sibling exactly once");
        assertEquals(
                1,
                sinkB.lastCreatedWriter.invocationCount.get(),
                "the lazy sibling must receive the schema change after being initialized");
    }

    /**
     * A connector that incorrectly returns a null Optional must not crash schema-change routing.
     * The coordinator should treat that buggy value as "no physical sink id" and keep the legacy
     * source-only dispatch intact.
     */
    @Test
    void nullPhysicalSinkIdentifierFallsBackToLegacyRouting() throws IOException {
        NullReturningPhysicalSinkWriter sinkA = new NullReturningPhysicalSinkWriter();
        RecordingSinkWriter sinkB = new RecordingSinkWriter("dbB.users", PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkA.invocationCount.get());
        assertEquals(
                0,
                sinkB.invocationCount.get(),
                "a null physical-sink identifier must simply disable sibling broadcast");
    }

    /**
     * Once a TTL wrapper has cached the physical sink identifier, closing the inner writer must not
     * force a second prepare during shared-sink discovery. The sibling should be recreated only for
     * the actual schema-change dispatch itself.
     */
    @Test
    void ttlClosedSiblingReusesCachedPhysicalSinkIdentifierDuringBroadcast() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        RecordingSeaTunnelSink sinkA =
                new RecordingSeaTunnelSink("dbA.users", PHYSICAL_SINK_SHARED);
        RecordingSeaTunnelSink sinkB =
                new RecordingSeaTunnelSink("dbB.users", PHYSICAL_SINK_SHARED);
        MultiTableTtlWriter ttlA =
                new MultiTableTtlWriter(
                        writers, "dbA.users", 0, 1, sinkA, new TestSinkWriterContext(), 60);
        MultiTableTtlWriter ttlB =
                new MultiTableTtlWriter(
                        writers, "dbB.users", 0, 1, sinkB, new TestSinkWriterContext(), 60);
        writers.put(SinkIdentifier.of("dbA.users", 0), ttlA);
        writers.put(SinkIdentifier.of("dbB.users", 0), ttlB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        assertEquals(Optional.of(PHYSICAL_SINK_SHARED), ttlB.getPhysicalSinkTableIdentifier());
        assertEquals(
                1,
                sinkB.createCount.get(),
                "the sibling prepares once while caching its physical sink identifier");

        ttlB.close();
        assertEquals(
                Optional.of(PHYSICAL_SINK_SHARED),
                ttlB.getPhysicalSinkTableIdentifier(),
                "the cached physical sink identifier must survive TTL close");
        assertEquals(
                1,
                sinkB.createCount.get(),
                "reading the cached identifier after close must not recreate the writer");

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(
                2,
                sinkB.createCount.get(),
                "the TTL-closed sibling should be recreated exactly once for actual dispatch");
        assertEquals(
                1,
                sinkB.lastCreatedWriter.invocationCount.get(),
                "the recreated sibling must still receive the schema change");
    }

    /**
     * Rows that were already queued before the schema change must still be written first. The new
     * in-band barrier path keeps the original queue order instead of letting schema change bypass
     * older rows and rebuild the writer too early.
     */
    @Test
    void queuedRowsDrainBeforeSchemaChangeMutatesTheWriter() throws Exception {
        OrderedRecordingSinkWriter sink = new OrderedRecordingSinkWriter("dbA.users");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            coordinator.write(row);
            schemaChangeThread =
                    new Thread(
                            () -> {
                                schemaChangeSubmitted.countDown();
                                try {
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users")));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            schemaChangeThread.start();
            assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(
                    sink.callOrder.isEmpty(),
                    "the queue worker must stay blocked until we release the runnable monitor");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(schemaChangeThread.isAlive(), "schema change thread must finish");
        assertEquals(
                Arrays.asList("row", "schema"),
                sink.callOrder,
                "older queued rows must be consumed before the schema change runs");
    }

    /**
     * The first schema change after startup must also enter the in-band barrier path. Otherwise a
     * concurrent bootstrap write could submit the queue workers while this DDL still mutates the
     * sink directly, reintroducing the ordering hole fixed by issue #4252.
     */
    @Test
    void firstSchemaChangeAfterStartupStillWaitsForTheQueueBarrier() throws Exception {
        RecordingSinkWriter sink = new RecordingSinkWriter("dbA.users", PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            schemaChangeThread =
                    new Thread(
                            () -> {
                                schemaChangeSubmitted.countDown();
                                try {
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users")));
                                } catch (Throwable throwable) {
                                    schemaChangeFailure.set(throwable);
                                }
                            });
            schemaChangeThread.start();
            assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(200);
            assertEquals(
                    0,
                    sink.invocationCount.get(),
                    "the first schema change must stay queued behind the runnable monitor");
            assertTrue(
                    schemaChangeFailure.get() == null,
                    "the startup schema change should block on the shared barrier instead of failing");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(schemaChangeThread.isAlive(), "schema change thread must finish after release");
        assertTrue(
                schemaChangeFailure.get() == null,
                "the queued startup schema change should complete successfully");
        assertEquals(
                1, sink.invocationCount.get(), "the startup schema change must run exactly once");
    }

    /**
     * If a queued old-schema row fails before the barrier is reached, applySchemaChange must fail
     * fast with the original write error instead of waiting forever for a dead queue worker.
     */
    @Test
    void schemaChangeFailsFastWhenQueuedRowWriteFailsBeforeBarrier() throws Exception {
        IOException rowWriteFailure = new IOException("boom-before-barrier");
        FailingRowSinkWriter sink = new FailingRowSinkWriter("dbA.users", rowWriteFailure);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            coordinator.write(row);
            schemaChangeThread =
                    new Thread(
                            () -> {
                                schemaChangeSubmitted.countDown();
                                try {
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users")));
                                } catch (Throwable throwable) {
                                    schemaChangeFailure.set(throwable);
                                }
                            });
            schemaChangeThread.start();
            assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(
                    schemaChangeFailure.get() == null,
                    "schema change should still be waiting on the shared barrier before release");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(
                schemaChangeThread.isAlive(),
                "schema change thread must fail fast instead of hanging behind a dead worker");
        assertTrue(
                schemaChangeFailure.get() instanceof IOException,
                "the original row-write failure should be surfaced back to applySchemaChange");
        assertEquals("boom-before-barrier", schemaChangeFailure.get().getMessage());
        assertEquals(0, sink.invocationCount.get(), "schema change must not run after row failure");
    }

    /**
     * Once a queue worker has already failed, applySchemaChange should keep the
     * SupportSchemaEvolutionSinkWriter IOException contract instead of leaking RuntimeException.
     */
    @Test
    void applySchemaChangeUsesIOExceptionContractWhenWorkerAlreadyFailed() throws Exception {
        RecordingSinkWriter sink = new RecordingSinkWriter("dbA.users", PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        IOException workerFailure = new IOException("existing-worker-failure");
        setWorkerThrowable(queueRunnable, workerFailure);

        IOException thrown =
                assertThrows(
                        IOException.class,
                        () ->
                                coordinator.applySchemaChange(
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users"))));
        assertSame(workerFailure, thrown);
    }

    /**
     * If barrier enqueue stops halfway because another queue has already failed, the already
     * enqueued workers must be released immediately instead of hanging until close interrupts them.
     */
    @Test
    void enqueueFailureFailsAlreadyQueuedSchemaBarrier() throws Exception {
        RecordingSinkWriter liveSink = new RecordingSinkWriter("dbA.users", PHYSICAL_SINK_SHARED);
        RecordingSinkWriter siblingSink = new RecordingSinkWriter("dbB.users", PHYSICAL_SINK_OTHER);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), liveSink);
        writers.put(SinkIdentifier.of("dbB.users", 1), siblingSink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 2, buildContextMap(writers));
        MultiTableWriterRunnable liveRunnable = getRunnable(coordinator, 0);
        MultiTableWriterRunnable blockedRunnable = getRunnable(coordinator, 1);
        BlockingQueue<MultiTableWriterRunnable.QueueElement> blockedQueue =
                getBlockingQueue(coordinator, 1);
        SeaTunnelRow filler = new SeaTunnelRow(new Object[] {1});
        filler.setTableId("dbB.users");
        while (blockedQueue.offer(MultiTableWriterRunnable.rowRequest(filler))) {
            // Fill the queue until the next schema barrier offer must enter the retry loop.
        }

        IOException enqueueFailure = new IOException("dead-queue-before-enqueue");
        markSubmitted(coordinator);
        getExecutorService(coordinator).submit(liveRunnable);

        Thread failureMarker =
                new Thread(
                        () -> {
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                                setWorkerThrowable(blockedRunnable, enqueueFailure);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        failureMarker.start();

        try {
            IOException thrown =
                    assertThrows(
                            IOException.class,
                            () ->
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users"))));
            assertSame(enqueueFailure, thrown);
            awaitWorkerFailure(liveRunnable);
            assertSame(
                    enqueueFailure,
                    liveRunnable.getThrowable(),
                    "the queue that already consumed the barrier must be released by fail-fast");
            assertEquals(
                    0,
                    liveSink.invocationCount.get(),
                    "dispatch must not run when the barrier never reached every queue");
        } finally {
            failureMarker.join(TimeUnit.SECONDS.toMillis(5));
            try {
                coordinator.close();
            } catch (Exception ignored) {
                // The test intentionally injects queue failure, so close may surface that failure.
            }
        }
    }

    /**
     * Schema maintenance should not count as row-write activity; otherwise a pure DDL heartbeat can
     * keep an idle TTL writer alive longer than configured.
     */
    @Test
    void schemaChangeDoesNotRefreshTtlWriteActivity() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        RecordingSeaTunnelSink sink = new RecordingSeaTunnelSink("dbA.users", PHYSICAL_SINK_SHARED);
        MultiTableTtlWriter ttlWriter =
                new MultiTableTtlWriter(
                        writers, "dbA.users", 0, 1, sink, new TestSinkWriterContext(), 1);
        writers.put(SinkIdentifier.of("dbA.users", 0), ttlWriter);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");
        ttlWriter.write(row);
        setLastWriteTime(ttlWriter, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5));

        ttlWriter.applySchemaChange(new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));
        ttlWriter.snapshotState(0);

        assertTrue(
                ttlWriter.isClosed(),
                "schema change alone should not keep an idle TTL writer alive past the TTL");
    }

    private static Map<SinkIdentifier, SinkWriter.Context> buildContextMap(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers) {
        Map<SinkIdentifier, SinkWriter.Context> ctxMap = new HashMap<>();
        writers.forEach((id, w) -> ctxMap.put(id, new TestSinkWriterContext()));
        return ctxMap;
    }

    /** Minimal {@link SinkWriter.Context} mirror of the existing connector-common unit tests. */
    private static class TestSinkWriterContext implements SinkWriter.Context {
        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return new DefaultEventProcessor();
        }
    }

    /** A minimal {@link SinkWriter} that records how many schema-change events it received. */
    private static class RecordingSinkWriter
            implements SinkWriter<SeaTunnelRow, Object, Object>,
                    SupportSchemaEvolutionSinkWriter,
                    SupportMultiTableSinkWriter<Object> {
        private final String sourceTableIdentifier;
        private final String physicalSinkIdentifier;
        final AtomicInteger invocationCount = new AtomicInteger(0);

        RecordingSinkWriter(String sourceTableIdentifier, String physicalSinkIdentifier) {
            this.sourceTableIdentifier = sourceTableIdentifier;
            this.physicalSinkIdentifier = physicalSinkIdentifier;
        }

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return Optional.ofNullable(physicalSinkIdentifier);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            invocationCount.incrementAndGet();
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {}

        @Override
        public Optional<Object> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public List<Object> snapshotState(long checkpointId) throws IOException {
            return SinkWriter.super.snapshotState(checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}

        @Override
        public MultiTableResourceManager<Object> initMultiTableResourceManager(
                int tableSize, int queueSize) {
            return new MultiTableResourceManager<Object>() {};
        }

        @Override
        public void setMultiTableResourceManager(
                MultiTableResourceManager multiTableResourceManager, int queueIndex) {}

        @Override
        public Optional<Integer> primaryKey() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "RecordingSinkWriter["
                    + sourceTableIdentifier
                    + "->"
                    + physicalSinkIdentifier
                    + "]";
        }
    }

    /** Buggy writer used to prove schema-change routing survives a null Optional implementation. */
    private static class NullReturningPhysicalSinkWriter extends RecordingSinkWriter {

        private NullReturningPhysicalSinkWriter() {
            super("dbA.users", PHYSICAL_SINK_SHARED);
        }

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return null;
        }
    }

    /**
     * Minimal sink factory that lets the TTL wrapper lazily create an inner writer while tests can
     * still assert when the writer was instantiated and whether it saw the schema change.
     */
    private static class RecordingSeaTunnelSink
            implements SeaTunnelSink<SeaTunnelRow, Object, Object, Object> {
        private final String sourceTableIdentifier;
        private final String physicalSinkIdentifier;
        private final AtomicInteger createCount = new AtomicInteger(0);
        private volatile RecordingSinkWriter lastCreatedWriter;

        private RecordingSeaTunnelSink(
                String sourceTableIdentifier, String physicalSinkIdentifier) {
            this.sourceTableIdentifier = sourceTableIdentifier;
            this.physicalSinkIdentifier = physicalSinkIdentifier;
        }

        @Override
        public SinkWriter<SeaTunnelRow, Object, Object> createWriter(SinkWriter.Context context) {
            createCount.incrementAndGet();
            lastCreatedWriter =
                    new RecordingSinkWriter(sourceTableIdentifier, physicalSinkIdentifier);
            return lastCreatedWriter;
        }

        @Override
        public String getPluginName() {
            return "recording-ttl-sink";
        }
    }

    /**
     * Legacy sub-writer that pre-dates the {@link SupportSchemaEvolutionSinkWriter} interface. Used
     * to verify that the broadcast logic skips writers it cannot identify safely.
     */
    private static class LegacySinkWriter
            implements SinkWriter<SeaTunnelRow, Object, Object>,
                    SupportMultiTableSinkWriter<Object> {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            invocationCount.incrementAndGet();
        }

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<Object> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public List<Object> snapshotState(long checkpointId) throws IOException {
            return SinkWriter.super.snapshotState(checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}

        @Override
        public MultiTableResourceManager<Object> initMultiTableResourceManager(
                int tableSize, int queueSize) {
            return new MultiTableResourceManager<Object>() {};
        }

        @Override
        public void setMultiTableResourceManager(
                MultiTableResourceManager multiTableResourceManager, int queueIndex) {}

        @Override
        public Optional<Integer> primaryKey() {
            return Optional.empty();
        }
    }

    /**
     * Captures the visible order between row writes and schema changes so backlog-ordering
     * regressions fail deterministically.
     */
    private static class OrderedRecordingSinkWriter extends RecordingSinkWriter {

        private final List<String> callOrder = new ArrayList<>();

        private OrderedRecordingSinkWriter(String sourceTableIdentifier) {
            super(sourceTableIdentifier, PHYSICAL_SINK_SHARED);
        }

        @Override
        public synchronized void applySchemaChange(SchemaChangeEvent event) {
            callOrder.add("schema");
            super.applySchemaChange(event);
        }

        @Override
        public synchronized void write(SeaTunnelRow element) {
            callOrder.add("row");
        }
    }

    /**
     * Writer used to prove a row failure ahead of the barrier fails schema change instead of
     * hanging.
     */
    private static class FailingRowSinkWriter extends RecordingSinkWriter {

        private final IOException writeFailure;

        private FailingRowSinkWriter(String sourceTableIdentifier, IOException writeFailure) {
            super(sourceTableIdentifier, PHYSICAL_SINK_SHARED);
            this.writeFailure = writeFailure;
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            throw writeFailure;
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static MultiTableWriterRunnable getRunnable(
            MultiTableSinkWriter coordinator, int queueIndex) throws Exception {
        Field runnableField = MultiTableSinkWriter.class.getDeclaredField("runnable");
        runnableField.setAccessible(true);
        List<MultiTableWriterRunnable> runnables =
                (List<MultiTableWriterRunnable>) runnableField.get(coordinator);
        return runnables.get(queueIndex);
    }

    /** Returns one queue backing the coordinator so tests can inject backlog and barrier races. */
    @SuppressWarnings("unchecked")
    private static BlockingQueue<MultiTableWriterRunnable.QueueElement> getBlockingQueue(
            MultiTableSinkWriter coordinator, int queueIndex) throws Exception {
        Field blockingQueuesField = MultiTableSinkWriter.class.getDeclaredField("blockingQueues");
        blockingQueuesField.setAccessible(true);
        List<BlockingQueue<MultiTableWriterRunnable.QueueElement>> blockingQueues =
                (List<BlockingQueue<MultiTableWriterRunnable.QueueElement>>)
                        blockingQueuesField.get(coordinator);
        return blockingQueues.get(queueIndex);
    }

    /** Exposes the coordinator executor so a test can start only the specific worker it needs. */
    private static ExecutorService getExecutorService(MultiTableSinkWriter coordinator)
            throws Exception {
        Field executorServiceField = MultiTableSinkWriter.class.getDeclaredField("executorService");
        executorServiceField.setAccessible(true);
        return (ExecutorService) executorServiceField.get(coordinator);
    }

    /** Marks the coordinator as already submitted so applySchemaChange takes the in-band path. */
    private static void markSubmitted(MultiTableSinkWriter coordinator) throws Exception {
        Field submittedField = MultiTableSinkWriter.class.getDeclaredField("submitted");
        submittedField.setAccessible(true);
        submittedField.setBoolean(coordinator, true);
    }

    /** Starts every queue worker so tests can exercise the same runtime path as real writes. */
    @SuppressWarnings("unchecked")
    private static void startQueueWorkers(MultiTableSinkWriter coordinator) throws Exception {
        markSubmitted(coordinator);
        Field runnableField = MultiTableSinkWriter.class.getDeclaredField("runnable");
        runnableField.setAccessible(true);
        List<MultiTableWriterRunnable> runnables =
                (List<MultiTableWriterRunnable>) runnableField.get(coordinator);
        ExecutorService executorService = getExecutorService(coordinator);
        for (MultiTableWriterRunnable worker : runnables) {
            executorService.submit(worker);
        }
    }

    /** Injects one worker failure so tests can cover coordinator fail-fast branches directly. */
    private static void setWorkerThrowable(MultiTableWriterRunnable worker, Throwable throwable)
            throws Exception {
        Field throwableField = MultiTableWriterRunnable.class.getDeclaredField("throwable");
        throwableField.setAccessible(true);
        throwableField.set(worker, throwable);
    }

    /**
     * Rewinds the TTL activity timestamp so tests can prove DDL no longer extends writer liveness.
     */
    private static void setLastWriteTime(MultiTableTtlWriter ttlWriter, long lastWriteTime)
            throws Exception {
        Field lastWriteTimeField = MultiTableTtlWriter.class.getDeclaredField("lastWriteTime");
        lastWriteTimeField.setAccessible(true);
        lastWriteTimeField.set(ttlWriter, lastWriteTime);
    }

    /**
     * Waits until one queue worker exits with failure instead of hanging behind a half-enqueued
     * barrier.
     */
    private static void awaitWorkerFailure(MultiTableWriterRunnable worker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (worker.getThrowable() == null && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        assertNotNull(
                worker.getThrowable(),
                "the worker that already consumed the barrier must not stay parked forever");
    }

    /** Minimal schema-change event that only carries a source {@link TablePath}. */
    private static class TestSchemaChangeEvent implements SchemaChangeEvent {
        private final TablePath tablePath;

        TestSchemaChangeEvent(TablePath tablePath) {
            this.tablePath = tablePath;
        }

        @Override
        public TablePath tablePath() {
            return tablePath;
        }

        @Override
        public TableIdentifier tableIdentifier() {
            return TableIdentifier.of("test", tablePath);
        }

        @Override
        public CatalogTable getChangeAfter() {
            return null;
        }

        @Override
        public void setChangeAfter(CatalogTable table) {}

        @Override
        public EventType getEventType() {
            return EventType.SCHEMA_CHANGE_UPDATE_COLUMNS;
        }

        @Override
        public long getCreatedTime() {
            return 0;
        }

        @Override
        public String getJobId() {
            return "test-job";
        }

        @Override
        public void setJobId(String jobId) {}
    }
}
