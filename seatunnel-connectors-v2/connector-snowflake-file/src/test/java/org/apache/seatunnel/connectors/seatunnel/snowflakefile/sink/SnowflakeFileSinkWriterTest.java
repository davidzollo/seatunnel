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
package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileSinkState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class SnowflakeFileSinkWriterTest {

    @TempDir private Path tempDir;

    @Test
    void snapshotStateKeepsPreparedFilesRecoverableUntilCommit() throws Exception {
        SnowflakeFileConfig config = localFileConfig();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new BasicType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        SnowflakeFileSinkWriter writer = new SnowflakeFileSinkWriter(config, rowType, context());

        writer.write(new SeaTunnelRow(new Object[] {1, "Alice"}));
        Optional<SnowflakeFileCommitInfo> commitInfo = writer.prepareCommit();
        List<SnowflakeFileSinkState> states = writer.snapshotState(1L);

        assertTrue(commitInfo.isPresent());
        assertEquals(1, states.size());
        assertEquals(commitInfo.get().getAllPartitionFiles(), states.get(0).getPartitionFiles());

        SnowflakeFileSinkWriter restoredWriter =
                new SnowflakeFileSinkWriter(config, rowType, context(), states);

        Optional<SnowflakeFileCommitInfo> restoredCommitInfo = restoredWriter.prepareCommit();

        assertTrue(restoredCommitInfo.isPresent());
        assertEquals(
                commitInfo.get().getAllPartitionFiles(),
                restoredCommitInfo.get().getAllPartitionFiles());

        writer.abortPrepare();
        restoredWriter.abortPrepare();
    }

    private SnowflakeFileConfig localFileConfig() {
        SnowflakeFileConfig config = Mockito.mock(SnowflakeFileConfig.class);
        when(config.isLocalFileStagingBackend()).thenReturn(true);
        when(config.getLocalTempDir()).thenReturn(tempDir.toString());
        when(config.getFieldDelimiter()).thenReturn(",");
        when(config.getRecordDelimiter()).thenReturn("\n");
        when(config.getFileExtension()).thenReturn(".csv");
        when(config.getMaxFileSize()).thenReturn(1024 * 1024L);
        return config;
    }

    private SinkWriter.Context context() {
        return new SinkWriter.Context() {
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
                return null;
            }
        };
    }
}
