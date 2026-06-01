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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.client.SnowflakeCopyIntoClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnowflakeFileCatalogTest {

    @Mock private SnowflakeFileConfig config;
    @Mock private SnowflakeCopyIntoClient client;
    @Mock private Connection connection;
    @Mock private DatabaseMetaData metaData;
    @Mock private ResultSet columnsResultSet;
    @Mock private ResultSet primaryKeysResultSet;
    @Mock private ResultSet tablesResultSet;

    @Test
    void getTableShouldReturnCatalogTableFromSnowflakeMetadata() throws Exception {
        TablePath tablePath = TablePath.of("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL");

        SnowflakeFileCatalog catalog = new SnowflakeFileCatalog(config);
        setField(catalog, "client", client);

        when(client.executeCountQuery(anyString())).thenReturn(1);
        when(client.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getColumns("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL", null))
                .thenReturn(columnsResultSet);
        when(metaData.getPrimaryKeys("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL"))
                .thenReturn(primaryKeysResultSet);
        when(metaData.getTables("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL", null))
                .thenReturn(tablesResultSet);

        when(columnsResultSet.next()).thenReturn(true, true, false);
        when(columnsResultSet.getString("COLUMN_NAME")).thenReturn("ID", "NAME");
        when(columnsResultSet.getString("TYPE_NAME")).thenReturn("BIGINT", "VARCHAR");
        when(columnsResultSet.getLong("COLUMN_SIZE")).thenReturn(19L, 255L);
        when(columnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(0, 0);
        when(columnsResultSet.getString("REMARKS")).thenReturn("primary id", "display name");
        when(columnsResultSet.getObject("COLUMN_DEF")).thenReturn(null, null);
        when(columnsResultSet.getInt("NULLABLE"))
                .thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
        when(columnsResultSet.wasNull()).thenReturn(false, false);

        when(primaryKeysResultSet.next()).thenReturn(true, false);
        when(primaryKeysResultSet.getString("COLUMN_NAME")).thenReturn("ID");
        when(primaryKeysResultSet.getString("PK_NAME")).thenReturn("PK_WT_FILE_WEB_SMALL");

        when(tablesResultSet.next()).thenReturn(true, false);
        when(tablesResultSet.getString("REMARKS")).thenReturn("small snowflake file table");

        CatalogTable catalogTable = catalog.getTable(tablePath);

        assertNotNull(catalogTable);
        assertEquals("small snowflake file table", catalogTable.getComment());
        assertIterableEquals(
                java.util.Arrays.asList("ID", "NAME"),
                catalogTable.getTableSchema().getColumnNames());
        assertIterableEquals(
                java.util.Collections.singletonList("ID"),
                catalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        assertEquals(
                SqlType.BIGINT,
                catalogTable.getTableSchema().getColumn("ID").getDataType().getSqlType());
        assertEquals(
                SqlType.STRING,
                catalogTable.getTableSchema().getColumn("NAME").getDataType().getSqlType());
    }

    @Test
    void createTableShouldQuoteIdentifiersAndPreserveOriginalCase() throws Exception {
        TablePath tablePath = TablePath.of("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL");
        List<Column> columns =
                Collections.singletonList(
                        PhysicalColumn.of(
                                "name",
                                org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                                null,
                                null,
                                true,
                                null,
                                null));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("WHALEOPSDB", "PUBLIC", "WT_FILE_WEB_SMALL"),
                        TableSchema.builder().columns(columns).build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "",
                        "SnowflakeFile");

        SnowflakeFileCatalog catalog = new SnowflakeFileCatalog(config);
        setField(catalog, "client", client);

        catalog.createTable(tablePath, catalogTable, true);

        verify(client)
                .executeSql(
                        "CREATE TABLE IF NOT EXISTS \"WHALEOPSDB\".\"PUBLIC\".\"WT_FILE_WEB_SMALL\" (\"name\" STRING)");
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
