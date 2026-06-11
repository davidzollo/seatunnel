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

/**
 * Records one concrete sub-writer that must observe the current schema change, together with the
 * reason why the coordinator selected it.
 */
final class SchemaChangeDispatchTarget {

    /** Identifies the exact sub-writer instance that should receive the schema change. */
    private final SinkIdentifier sinkIdentifier;
    /** Holds the sub-writer that will execute the schema change. */
    private final SinkWriter<SeaTunnelRow, ?, ?> writer;
    /** Explains whether the target came from the source match or a shared physical sink match. */
    private final String reason;

    /** Creates one immutable schema-change dispatch target for the selected sub-writer. */
    SchemaChangeDispatchTarget(
            SinkIdentifier sinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?> writer, String reason) {
        this.sinkIdentifier = sinkIdentifier;
        this.writer = writer;
        this.reason = reason;
    }

    /** Returns the exact sink identifier selected for the current schema change. */
    SinkIdentifier getSinkIdentifier() {
        return sinkIdentifier;
    }

    /** Returns the sub-writer that should execute the schema change. */
    SinkWriter<SeaTunnelRow, ?, ?> getWriter() {
        return writer;
    }

    /** Returns the selection reason for log messages and test assertions. */
    String getReason() {
        return reason;
    }
}
