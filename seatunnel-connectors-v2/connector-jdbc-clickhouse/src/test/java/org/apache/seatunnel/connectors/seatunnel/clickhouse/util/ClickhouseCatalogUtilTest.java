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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickhouseCatalogUtilTest {

    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        PhysicalColumn column =
                PhysicalColumn.of(
                        "col1", BasicType.INT_TYPE, null, null, true, null, null, "String", null);

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` Nullable(String) ", result);
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeIsNull() {
        PhysicalColumn column =
                PhysicalColumn.of("col1", BasicType.INT_TYPE, null, null, true, null, null);

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` Nullable(Int32) ", result);
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        PhysicalColumn column =
                PhysicalColumn.of(
                        "col1", BasicType.INT_TYPE, null, null, true, null, null, "String", null);

        String result = ClickhouseCatalogUtil.INSTANCE.columnToConnectorType(column);

        assertEquals("`col1` Nullable(String) ", result);
    }

    @Test
    void injectsIndexDefinitionsWhenOptionsProvided() {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, false, null, null));
        builder.column(
                PhysicalColumn.of("status", BasicType.STRING_TYPE, null, null, true, null, null));

        ConstraintKey.ConstraintKeyColumn statusColumn =
                ConstraintKey.ConstraintKeyColumn.of("status", ConstraintKey.ColumnSortType.ASC);
        ConstraintKey indexKey =
                ConstraintKey.of(
                        ConstraintKey.ConstraintType.INDEX_KEY,
                        "idx_status",
                        Collections.singletonList(statusColumn));
        builder.constraintKey(indexKey);

        // primary key is required by default ClickHouse template
        builder.primaryKey(PrimaryKey.of("pk_id", Collections.singletonList("id")));

        TableSchema tableSchema = builder.build();

        Map<String, String> options = new HashMap<>();
        String baseKey = ClickhouseSinkOptions.INDEX_OPTION_PREFIX + "idx_status";
        options.put(baseKey + ClickhouseSinkOptions.INDEX_OPTION_EXPR_SUFFIX, "status");
        options.put(baseKey + ClickhouseSinkOptions.INDEX_OPTION_TYPE_FULL_SUFFIX, "set(0)");
        options.put(baseKey + ClickhouseSinkOptions.INDEX_OPTION_GRANULARITY_SUFFIX, "1");

        String template = ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();

        String ddl =
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        template,
                        "wt_dev",
                        "full_type_table",
                        tableSchema,
                        "comment",
                        options,
                        ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());

        assertTrue(
                ddl.contains("INDEX idx_status status TYPE set(0) GRANULARITY 1"),
                "DDL should contain full ClickHouse index definition");
        assertTrue(
                ddl.contains("ENGINE = ReplacingMergeTree()"),
                "DDL should still contain engine definition");
    }

    @Test
    void doesNotInjectIndexDefinitionsWithoutOptions() {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, false, null, null));
        builder.column(
                PhysicalColumn.of("status", BasicType.STRING_TYPE, null, null, true, null, null));

        ConstraintKey.ConstraintKeyColumn statusColumn =
                ConstraintKey.ConstraintKeyColumn.of("status", ConstraintKey.ColumnSortType.ASC);
        ConstraintKey indexKey =
                ConstraintKey.of(
                        ConstraintKey.ConstraintType.INDEX_KEY,
                        "idx_status",
                        Collections.singletonList(statusColumn));
        builder.constraintKey(indexKey);

        // primary key is required by default ClickHouse template
        builder.primaryKey(PrimaryKey.of("pk_id", Collections.singletonList("id")));

        TableSchema tableSchema = builder.build();

        Map<String, String> emptyOptions = new HashMap<>();
        String template = ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();

        String ddl =
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        template,
                        "wt_dev",
                        "full_type_table",
                        tableSchema,
                        "comment",
                        emptyOptions,
                        ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());

        assertFalse(
                ddl.contains("INDEX idx_status"),
                "DDL should not contain index definitions when options are missing");
    }

    @Test
    void quotesTableIdentifierInDropAndTruncateSql() {
        TablePath tablePath = TablePath.of("st3355_empty_db", "9-1_users1");

        assertEquals(
                "DROP TABLE IF EXISTS `st3355_empty_db`.`9-1_users1`",
                ClickhouseCatalogUtil.INSTANCE.getDropTableSql(tablePath, true));
        assertEquals(
                "TRUNCATE TABLE `st3355_empty_db`.`9-1_users1`",
                ClickhouseCatalogUtil.INSTANCE.getTruncateTableSql(tablePath));
    }
}
