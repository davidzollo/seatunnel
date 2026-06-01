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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LocalFileWriterTest {

    @Mock private SnowflakeFileConfig config;

    @TempDir private Path tempDir;

    @Test
    public void testWriteFlushAndCleanup() throws IOException {
        when(config.getLocalTempDir()).thenReturn(tempDir.toString());
        when(config.getFieldDelimiter()).thenReturn(",");
        when(config.getRecordDelimiter()).thenReturn("\n");
        when(config.getFileExtension()).thenReturn(".csv");
        when(config.getMaxFileSize()).thenReturn(1024 * 1024L);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new BasicType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        LocalFileWriter writer = new LocalFileWriter(config, rowType, "writer-1");

        SeaTunnelRow firstRow = new SeaTunnelRow(new Object[] {1, "Alice"});
        SeaTunnelRow secondRow = new SeaTunnelRow(new Object[] {2, "Bob"});
        writer.writeRow(firstRow, "partition-1");
        writer.writeRow(secondRow, "partition-1");
        writer.flushAll();

        List<String> files = writer.getUploadedFiles("partition-1");
        assertEquals(1, files.size());

        Path filePath = Paths.get(files.get(0));
        assertTrue(Files.exists(filePath));
        String content = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
        assertTrue(content.contains("1,Alice"));
        assertTrue(content.contains("2,Bob"));

        writer.cleanupFiles(files);
        assertFalse(Files.exists(filePath));
    }
}
