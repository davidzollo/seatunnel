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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.client;

import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SnowflakeCopyIntoClientTest {

    @Mock private SnowflakeFileConfig config;
    @Mock private Connection connection;
    @Mock private Statement warehouseStatement;
    @Mock private Statement databaseStatement;
    @Mock private Statement schemaStatement;

    @TempDir private Path tempDir;

    @Test
    public void testBuildLocalFileCommands() throws Exception {
        when(config.getDatabase()).thenReturn("TEST_DB");
        when(config.getSchema()).thenReturn("PUBLIC");
        when(config.getTable()).thenReturn("ORDERS");
        when(config.getFileFormat()).thenReturn("CSV");
        when(config.getFieldDelimiter()).thenReturn(",");
        when(config.getRecordDelimiter()).thenReturn("\n");
        when(config.isPurgeAfterCopy()).thenReturn(true);
        when(config.isLocalFileStagingBackend()).thenReturn(true);
        when(config.getLocalStageType()).thenReturn(SnowflakeFileConfig.LocalStageType.USER);
        when(config.getLocalStagePrefix()).thenReturn("seatunnel-local");

        SnowflakeCopyIntoClient client = new SnowflakeCopyIntoClient(config);

        Path localFile = Files.createFile(tempDir.resolve("orders.csv"));
        String stageLocation = client.buildLocalStageLocation();
        String putSql = client.buildPutCommand(localFile, stageLocation);
        String copySql =
                client.buildCopyIntoStageSql(
                        stageLocation,
                        Collections.singletonList(localFile.getFileName().toString()));

        assertTrue(putSql.startsWith("PUT 'file://"));
        assertTrue(putSql.contains("@~/seatunnel-local/"));
        assertTrue(putSql.contains("OVERWRITE = TRUE"));
        assertTrue(copySql.contains("COPY INTO \"TEST_DB\".\"PUBLIC\".\"ORDERS\""));
        assertTrue(copySql.contains("FROM " + stageLocation + "/"));
        assertTrue(copySql.contains("FILES = ('orders.csv')"));
        assertTrue(copySql.contains("FILE_FORMAT = (TYPE = 'CSV'"));
        assertTrue(copySql.contains("ON_ERROR = 'ABORT_STATEMENT'"));
    }

    @Test
    public void testInitializeSessionContextShouldUseWarehouseDatabaseAndSchema() throws Exception {
        when(config.getWarehouse()).thenReturn("COMPUTE_WH");
        when(config.getDatabase()).thenReturn("WHALEOPSDB");
        when(config.getSchema()).thenReturn("PUBLIC");
        when(connection.createStatement())
                .thenReturn(warehouseStatement, databaseStatement, schemaStatement);

        SnowflakeCopyIntoClient client = new SnowflakeCopyIntoClient(config);
        setField(client, "connection", connection);

        client.initializeSessionContext();

        verify(warehouseStatement, times(1)).execute("USE WAREHOUSE \"COMPUTE_WH\"");
        verify(databaseStatement, times(1)).execute("USE DATABASE \"WHALEOPSDB\"");
        verify(schemaStatement, times(1)).execute("USE SCHEMA \"PUBLIC\"");
    }

    @Test
    public void testInitializeSessionContextShouldSkipWarehouseWhenNotConfigured()
            throws Exception {
        when(config.getWarehouse()).thenReturn(null);
        when(config.getDatabase()).thenReturn("WHALEOPSDB");
        when(config.getSchema()).thenReturn("PUBLIC");
        when(connection.createStatement()).thenReturn(databaseStatement, schemaStatement);

        SnowflakeCopyIntoClient client = new SnowflakeCopyIntoClient(config);
        setField(client, "connection", connection);

        client.initializeSessionContext();

        verify(databaseStatement, times(1)).execute("USE DATABASE \"WHALEOPSDB\"");
        verify(schemaStatement, times(1)).execute("USE SCHEMA \"PUBLIC\"");
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
