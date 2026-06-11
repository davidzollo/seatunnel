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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

@Slf4j
public class MultiTableTtlWriter
        implements SinkWriter<SeaTunnelRow, Object, Object>,
                SupportResourceShare,
                SupportSchemaEvolutionSinkWriter {
    private Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final SeaTunnelSink sink;
    @Getter private final String tableIdentifier;
    private final int index;
    private final int replicaNum;
    private int queueIndex;
    private final SinkWriter.Context context;
    private volatile SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;
    /**
     * Caches the resolved physical sink table so shared-sink schema-change routing still works
     * after TTL closes the inner writer.
     */
    private volatile Optional<String> physicalSinkTableIdentifier = Optional.empty();
    /**
     * Marks whether the wrapper has already resolved the physical sink identifier at least once.
     */
    private volatile boolean physicalSinkIdentifierInitialized;

    private volatile Long lastWriteTime;
    private volatile Optional<Integer> primaryKey;
    private volatile MultiTableResourceManager multiTableResourceManager;
    private List<?> state;
    private final int multiTableWriterTtl;
    @Getter private boolean isClosed = true;

    public MultiTableTtlWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            String tableIdentifier,
            int index,
            int replicaNum,
            SeaTunnelSink sink,
            SinkWriter.Context context,
            int multiTableWriterTtl) {
        this.sinkWriters = sinkWriters;
        this.sink = sink;
        this.tableIdentifier = tableIdentifier;
        this.index = index;
        this.replicaNum = replicaNum;
        this.context = context;
        this.multiTableWriterTtl = multiTableWriterTtl;
        this.lastWriteTime = System.currentTimeMillis();
    }

    public MultiTableTtlWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            String tableIdentifier,
            int index,
            int replicaNum,
            SeaTunnelSink sink,
            SinkWriter.Context context,
            int multiTableWriterTtl,
            List<?> state) {
        this(sinkWriters, tableIdentifier, index, replicaNum, sink, context, multiTableWriterTtl);
        this.state = state;
    }

    /**
     * Ensures the inner writer exists without treating metadata access or schema maintenance as row
     * write activity. TTL should track real row writes only.
     */
    public synchronized SinkWriter<SeaTunnelRow, ?, ?> prepare() {
        if (sinkWriter == null) {
            try {
                log.info("Create writer for table {} with index {}", tableIdentifier, index);
                if (state == null) {
                    sinkWriter =
                            sink.createWriter(new SinkContextProxy(index, replicaNum, context));
                } else {
                    sinkWriter =
                            sink.restoreWriter(
                                    new SinkContextProxy(index, replicaNum, context), state);
                }
                isClosed = false;
                if (sinkWriter instanceof SupportMultiTableSinkWriter
                        && multiTableResourceManager != null) {
                    SupportMultiTableSinkWriter<?> sink =
                            ((SupportMultiTableSinkWriter<?>) sinkWriter);
                    sink.setMultiTableResourceManager(multiTableResourceManager, queueIndex);
                }
                cachePhysicalSinkTableIdentifier(sinkWriter);
                traceLogAllWriters();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return sinkWriter;
    }

    public SinkWriter<SeaTunnelRow, ?, ?> create() {
        return prepare();
    }

    public Optional<Integer> primaryKey() {
        if (primaryKey != null) {
            return primaryKey;
        }
        prepare();
        if (sinkWriter instanceof SupportMultiTableSinkWriter) {
            SupportMultiTableSinkWriter<?> supportMultiTableSinkWriter =
                    (SupportMultiTableSinkWriter<?>) sinkWriter;
            primaryKey = supportMultiTableSinkWriter.primaryKey();
        } else {
            primaryKey = Optional.empty();
        }
        return primaryKey;
    }

    public void setMultiTableResourceManager(
            MultiTableResourceManager multiTableResourceManager, int queueIndex) {
        if (this.multiTableResourceManager == null) {
            this.multiTableResourceManager = multiTableResourceManager;
            if (sinkWriter != null && sinkWriter instanceof SupportMultiTableSinkWriter) {
                SupportMultiTableSinkWriter<?> sink = ((SupportMultiTableSinkWriter<?>) sinkWriter);
                sink.setMultiTableResourceManager(multiTableResourceManager, queueIndex);
            }
        }
        this.queueIndex = queueIndex;
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        prepare();
        touchLastWriteTime();
        sinkWriter.write(row);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        prepare();
        if (sinkWriter instanceof SupportSchemaEvolutionSinkWriter) {
            ((SupportSchemaEvolutionSinkWriter) sinkWriter).applySchemaChange(event);
        } else {
            // TODO remove deprecated method
            sinkWriter.applySchemaChange(event);
        }
    }

    /**
     * Delegates to the inner writer's physical-sink identifier so the multi-table coordinator can
     * still detect shared physical sink tables when the wrapper sits in front of a writer that
     * resolves a sink-table template per upstream source (issue #4252). Lazy / TTL-closed writers
     * are prepared on demand once so the coordinator can keep broadcasting schema changes to every
     * sibling writer that shares the same destination table. The coordinator probes this method
     * either before the queue workers are submitted, or while every queue runnable is already
     * frozen at the schema-change barrier; in both cases the lazy prepare path stays serialized
     * with normal writes.
     */
    @Override
    public Optional<String> getPhysicalSinkTableIdentifier() {
        if (physicalSinkIdentifierInitialized) {
            return physicalSinkTableIdentifier;
        }
        SinkWriter<SeaTunnelRow, ?, ?> current = sinkWriter;
        if (current == null) {
            current = prepare();
        }
        cachePhysicalSinkTableIdentifier(current);
        return physicalSinkTableIdentifier;
    }

    @Override
    public Optional<Object> prepareCommit() throws IOException {
        if (sinkWriter != null) {
            return (Optional<Object>) sinkWriter.prepareCommit();
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        if (sinkWriter != null) {
            sinkWriter.abortPrepare();
        }
    }

    @Override
    public List snapshotState(long checkpointId) throws IOException {
        if (sinkWriter != null) {
            List state = sinkWriter.snapshotState(checkpointId);

            // handle ttl
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastWriteTime > (long) multiTableWriterTtl * 1000L) {
                log.info(
                        "Close writer for table {} with index {} because of ttl",
                        tableIdentifier,
                        index);
                sinkWriter.close();
                sinkWriter = null;
                lastWriteTime = currentTime;
                isClosed = true;
                traceLogAllWriters();
            }
            return state;
        }
        return Collections.emptyList();
    }

    /** Marks that a real data row has just used the inner writer, which should refresh TTL. */
    private void touchLastWriteTime() {
        lastWriteTime = System.currentTimeMillis();
    }

    /**
     * Resolves the inner writer's physical sink identifier once and keeps the result even after the
     * writer gets TTL-closed, so later schema changes can still fan out to this sibling writer.
     */
    private void cachePhysicalSinkTableIdentifier(SinkWriter<SeaTunnelRow, ?, ?> current) {
        if (physicalSinkIdentifierInitialized) {
            return;
        }
        if (current instanceof SupportSchemaEvolutionSinkWriter) {
            Optional<String> physicalSinkIdentifier =
                    ((SupportSchemaEvolutionSinkWriter) current).getPhysicalSinkTableIdentifier();
            physicalSinkTableIdentifier =
                    physicalSinkIdentifier == null ? Optional.empty() : physicalSinkIdentifier;
        } else {
            physicalSinkTableIdentifier = Optional.empty();
        }
        physicalSinkIdentifierInitialized = true;
    }

    private void traceLogAllWriters() {
        StringJoiner sj = new StringJoiner("\n");
        sinkWriters.forEach(
                (k, v) -> {
                    MultiTableTtlWriter multiTableTtlWriter = (MultiTableTtlWriter) v;
                    if (multiTableTtlWriter.isClosed()) {
                        sj.add(k.toString() + ", state: CLOSED");
                    } else {
                        sj.add(k.toString() + ", state: ACTIVE");
                    }
                });
        if (!sj.toString().isEmpty()) {
            log.info("Writers state: \n{}", sj);
        }
    }

    @Override
    public void close() throws IOException {
        if (sinkWriter != null) {
            sinkWriter.close();
            sinkWriter = null;
            lastWriteTime = System.currentTimeMillis();
            isClosed = true;
        }
    }
}
