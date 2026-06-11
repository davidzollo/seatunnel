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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Consumes ordered queue requests for one sink queue. Both data rows and schema-change barriers go
 * through this runnable so the worker always drains older rows before it switches the shared sink
 * schema.
 */
@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    private final Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap;
    private final BlockingQueue<QueueElement> queue;
    private volatile Throwable throwable;
    private volatile String currentTableId;

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue) {
        this.tableIdWriterMap = tableIdWriterMap;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            QueueElement queueElement = null;
            try {
                queueElement = queue.poll(100, TimeUnit.MILLISECONDS);
                if (queueElement == null) {
                    continue;
                }
                synchronized (this) {
                    queueElement.process(this);
                }
            } catch (InterruptedException e) {
                // When the job finished, the thread will be interrupted, so we ignore this
                // exception.
                throwable = e;
                failPendingSchemaChangeRequests(queueElement, e);
                break;
            } catch (Throwable e) {
                log.error(
                        String.format(
                                "MultiTableWriterRunnable error when process queue element %s",
                                queueElement),
                        e);
                throwable = e;
                failPendingSchemaChangeRequests(queueElement, e);
                break;
            }
        }
    }

    /**
     * Releases any queued schema-change barriers when this worker dies before reaching them. That
     * keeps the coordinator on the fail-fast path instead of letting applySchemaChange hang behind
     * a dead queue worker.
     */
    private void failPendingSchemaChangeRequests(QueueElement currentElement, Throwable failure) {
        if (currentElement != null) {
            currentElement.fail(failure);
        }
        for (QueueElement pendingElement : queue) {
            pendingElement.fail(failure);
        }
    }

    /** Applies one queued row write while the runnable monitor is already held by the worker. */
    void writeRow(SeaTunnelRow row) throws IOException {
        SinkWriter<SeaTunnelRow, ?, ?> writer = tableIdWriterMap.get(row.getTableId());
        if (writer == null) {
            if (tableIdWriterMap.size() == 1) {
                writer = tableIdWriterMap.values().stream().findFirst().get();
                currentTableId = tableIdWriterMap.keySet().stream().findFirst().get();
            } else {
                throw new RuntimeException(
                        "MultiTableWriterRunnable can't find writer for tableId: "
                                + row.getTableId());
            }
        } else {
            currentTableId = row.getTableId();
        }
        writer.write(row);
    }

    /**
     * Blocks this queue worker at the shared schema-change barrier. The barrier releases all queue
     * workers only after every earlier row has been drained and the coordinator finished mutating
     * the target writers' schema state.
     */
    void awaitSchemaChangeBarrier(SchemaChangeBarrier schemaChangeBarrier) throws IOException {
        schemaChangeBarrier.reachBarrier();
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getCurrentTableId() {
        return currentTableId;
    }

    /** Creates one ordered queue element that writes a data row. */
    static QueueElement rowRequest(SeaTunnelRow row) {
        return new RowWriteRequest(row);
    }

    /** Creates one ordered queue element that blocks on the shared schema-change barrier. */
    static QueueElement schemaChangeRequest(SchemaChangeBarrier schemaChangeBarrier) {
        return new SchemaChangeRequest(schemaChangeBarrier);
    }

    /**
     * Represents one ordered action in the queue: either a row write or a schema-change barrier.
     */
    interface QueueElement {

        void process(MultiTableWriterRunnable runnable) throws Exception;

        /** Allows worker shutdown paths to fail pending queue elements without processing them. */
        default void fail(Throwable failure) {}
    }

    private static class RowWriteRequest implements QueueElement {

        private final SeaTunnelRow row;

        private RowWriteRequest(SeaTunnelRow row) {
            this.row = row;
        }

        @Override
        public void process(MultiTableWriterRunnable runnable) throws IOException {
            runnable.writeRow(row);
        }

        @Override
        public String toString() {
            return "row[" + row + "]";
        }
    }

    private static class SchemaChangeRequest implements QueueElement {

        private final SchemaChangeBarrier schemaChangeBarrier;

        private SchemaChangeRequest(SchemaChangeBarrier schemaChangeBarrier) {
            this.schemaChangeBarrier = schemaChangeBarrier;
        }

        @Override
        public void process(MultiTableWriterRunnable runnable) throws IOException {
            runnable.awaitSchemaChangeBarrier(schemaChangeBarrier);
        }

        @Override
        public void fail(Throwable failure) {
            schemaChangeBarrier.fail(failure);
        }

        @Override
        public String toString() {
            return "schema-change[" + schemaChangeBarrier.getTablePath() + "]";
        }
    }
}
