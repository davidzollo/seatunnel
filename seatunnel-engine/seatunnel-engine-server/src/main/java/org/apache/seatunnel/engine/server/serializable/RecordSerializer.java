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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class RecordSerializer implements StreamSerializer<Record> {
    private static final byte TYPE_CHECKPOINT_BARRIER = 0;
    /**
     * Legacy SeaTunnelRow record type.
     *
     * <p>Keep this value for backward compatibility (new version must be able to read old data).
     */
    private static final byte TYPE_SEATUNNEL_ROW_V1 = 1;

    /**
     * SeaTunnelRow with complete options Map serialized.
     *
     * <p>Supports all key/value pairs stored in {@code SeaTunnelRow.options}, including StainTrace
     * payload and CDC metadata (e.g. EventTime, PARTITION, DATABASE, TABLE).
     *
     * <p>Invalid or oversized StainTrace payloads are stripped before serialization to avoid
     * transmitting garbage data.
     */
    private static final byte TYPE_SEATUNNEL_ROW_V3 = 3;

    private static final int MAX_TRACE_PAYLOAD_LENGTH = 8 * 1024;

    @Override
    public void write(ObjectDataOutput out, Record record) throws IOException {
        Object data = record.getData();
        if (data instanceof CheckpointBarrier) {
            CheckpointBarrier checkpointBarrier = (CheckpointBarrier) data;
            out.writeByte(TYPE_CHECKPOINT_BARRIER);
            out.writeLong(checkpointBarrier.getId());
            out.writeLong(checkpointBarrier.getTimestamp());
            out.writeString(checkpointBarrier.getCheckpointType().getName());
            out.writeObject(checkpointBarrier.getPrepareCloseTasks());
            out.writeObject(checkpointBarrier.getClosedTasks());
        } else if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            Map<String, Object> opts = buildSerializableOptions(row);
            out.writeByte(opts == null ? TYPE_SEATUNNEL_ROW_V1 : TYPE_SEATUNNEL_ROW_V3);
            out.writeString(row.getTableId());
            out.writeByte(row.getRowKind().toByteValue());
            out.writeByte(row.getArity());
            for (Object field : row.getFields()) {
                out.writeObject(field);
            }
            if (opts != null) {
                out.writeObject(opts);
            }
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported serialize class: " + data.getClass());
        }
    }

    @Override
    public Record read(ObjectDataInput in) throws IOException {
        Object data;
        byte dataType = in.readByte();
        if (dataType == TYPE_CHECKPOINT_BARRIER) {
            data =
                    new CheckpointBarrier(
                            in.readLong(),
                            in.readLong(),
                            CheckpointType.fromName(in.readString()),
                            in.readObject(),
                            in.readObject());
        } else if (dataType == TYPE_SEATUNNEL_ROW_V1 || dataType == TYPE_SEATUNNEL_ROW_V3) {
            String tableId = in.readString();
            byte rowKind = in.readByte();
            byte arity = in.readByte();
            SeaTunnelRow row = new SeaTunnelRow(arity);
            row.setTableId(tableId);
            row.setRowKind(RowKind.fromByteValue(rowKind));
            for (int i = 0; i < arity; i++) {
                row.setField(i, in.readObject());
            }
            if (dataType == TYPE_SEATUNNEL_ROW_V3) {
                Map<String, Object> opts = in.readObject();
                if (opts != null && !opts.isEmpty()) {
                    row.setOptions(opts);
                }
            }
            data = row;
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported deserialize data type: " + dataType);
        }
        return new Record(data);
    }

    /**
     * Builds the options map to be serialized with a row.
     *
     * <p>Returns {@code null} when the row has no options worth transmitting, so the caller can
     * fall back to the compact {@link #TYPE_SEATUNNEL_ROW_V1} format. Otherwise returns a shallow
     * copy of the row's options map with any invalid StainTrace payload removed (empty or exceeds
     * {@link #MAX_TRACE_PAYLOAD_LENGTH}).
     */
    private Map<String, Object> buildSerializableOptions(SeaTunnelRow row) {
        Map<String, Object> source = row.getOptionsOrNull();
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, Object> opts = new HashMap<>(source);
        Object payloadObj = opts.get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        if (payloadObj instanceof byte[]) {
            byte[] payload = (byte[]) payloadObj;
            if (payload.length <= 0 || payload.length > MAX_TRACE_PAYLOAD_LENGTH) {
                opts.remove(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
            }
        }
        return opts.isEmpty() ? null : opts;
    }

    @Override
    public int getTypeId() {
        return TypeId.RECORD;
    }
}
