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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.highgo.HighGoDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcCatalogUtilsTest {
    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            false,
                                            null,
                                            null,
                                            "int unsigned",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.STRING_TYPE,
                                            10,
                                            false,
                                            null,
                                            null,
                                            "varchar(10)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.STRING_TYPE,
                                            20,
                                            false,
                                            null,
                                            null,
                                            "varchar(20)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testColumnEqualsMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);
        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(DEFAULT_TABLE.getOptions(), mergeTable.getOptions());
        Assertions.assertEquals(DEFAULT_TABLE.getComment(), mergeTable.getComment());
        Assertions.assertEquals(DEFAULT_TABLE.getCatalogName(), mergeTable.getCatalogName());
        Assertions.assertNotEquals(DEFAULT_TABLE.getTableSchema(), mergeTable.getTableSchema());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Map<String, Column> columnMap =
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(e -> e.getName(), e -> e));
        List<Column> sortByQueryColumns =
                tableOfQuery.getTableSchema().getColumns().stream()
                        .map(e -> columnMap.get(e.getName()))
                        .collect(Collectors.toList());
        Assertions.assertEquals(sortByQueryColumns, mergeTable.getTableSchema().getColumns());
    }

    @Test
    public void testColumnIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .filter(c -> Arrays.asList("f1", "f3").contains(c.getName()))
                        .collect(Collectors.toList()),
                mergeTable.getTableSchema().getColumns());
        Assertions.assertTrue(mergeTable.getPartitionKeys().isEmpty());
        Assertions.assertTrue(mergeTable.getTableSchema().getConstraintKeys().isEmpty());
    }

    @Test
    public void testColumnNotIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f4",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(
                DEFAULT_TABLE.getTableId().toTablePath(), mergeTable.getTableId().toTablePath());
        Assertions.assertEquals(DEFAULT_TABLE.getPartitionKeys(), mergeTable.getPartitionKeys());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Assertions.assertEquals(
                tableOfQuery.getTableId().getCatalogName(),
                mergeTable.getTableId().getCatalogName());
        Assertions.assertEquals(
                tableOfQuery.getTableSchema().getColumns(),
                mergeTable.getTableSchema().getColumns());
    }

    /**
     * Verify that legacy origin/2.6-release PostgreSQL-family generated SQL no longer keeps the
     * database qualifier after table_path has selected the target database connection.
     */
    @Test
    public void testNormalizePostgresFamilyLegacyDatabaseQualifiedQuery() {
        TablePath tablePath = TablePath.of("qa_sink", "public", "BATCH_SAVEMODE_1");
        String sqlQuery =
                "SELECT \"id\", \"name\" FROM \"qa_sink\".\"public\".\"BATCH_SAVEMODE_1\" "
                        + "WHERE \"id\" > 100";

        String normalizedQuery =
                JdbcCatalogUtils.normalizePostgresFamilyTablePathQuery(
                        sqlQuery, tablePath, new PostgresDialect());

        Assertions.assertEquals(
                "SELECT \"id\", \"name\" FROM \"public\".\"BATCH_SAVEMODE_1\" WHERE \"id\" > 100",
                normalizedQuery);
    }

    /**
     * Verify that current origin/2.6-test PostgreSQL-family generated SQL is left untouched because
     * it already uses schema.table inside the table_path database connection.
     */
    @Test
    public void testNormalizePostgresFamilyKeepsSchemaQualifiedQuery() {
        TablePath tablePath = TablePath.of("qa_sink", "public", "BATCH_SAVEMODE_1");
        String sqlQuery = "SELECT \"id\", \"name\" FROM \"public\".\"BATCH_SAVEMODE_1\"";

        String normalizedQuery =
                JdbcCatalogUtils.normalizePostgresFamilyTablePathQuery(
                        sqlQuery, tablePath, new PostgresDialect());

        Assertions.assertEquals(sqlQuery, normalizedQuery);
    }

    /**
     * Verify that HighGo follows the same PostgreSQL-family compatibility rule for legacy Web
     * generated SQL.
     */
    @Test
    public void testNormalizeHighGoLegacyDatabaseQualifiedQuery() {
        TablePath tablePath = TablePath.of("qa_source", "public", "highgo_table");
        String sqlQuery = "select id from qa_source.public.highgo_table where id > 0";

        String normalizedQuery =
                JdbcCatalogUtils.normalizePostgresFamilyTablePathQuery(
                        sqlQuery, tablePath, new HighGoDialect());

        Assertions.assertEquals("select id from public.highgo_table where id > 0", normalizedQuery);
    }

    /**
     * Verify that incomplete table_path metadata is ignored without relying on newer commons-lang3
     * varargs helpers that are absent from the origin/2.6 runtime.
     */
    @Test
    public void testNormalizePostgresFamilySkipsBlankTablePathSegments() {
        TablePath tablePath = TablePath.of(null, "public", "BATCH_SAVEMODE_1");
        String sqlQuery = "SELECT \"id\" FROM \"qa_sink\".\"public\".\"BATCH_SAVEMODE_1\"";

        String normalizedQuery =
                JdbcCatalogUtils.normalizePostgresFamilyTablePathQuery(
                        sqlQuery, tablePath, new PostgresDialect());

        Assertions.assertEquals(sqlQuery, normalizedQuery);
    }

    /**
     * Verify that database-qualified SQL for a different table is not rewritten accidentally.
     *
     * <p>This protects custom query text that does not refer to the configured table_path table.
     */
    @Test
    public void testNormalizePostgresFamilyKeepsDifferentRelation() {
        TablePath tablePath = TablePath.of("qa_sink", "public", "BATCH_SAVEMODE_1");
        String sqlQuery =
                "SELECT \"id\" FROM \"qa_sink\".\"public\".\"OTHER_TABLE\" WHERE \"id\" > 0";

        String normalizedQuery =
                JdbcCatalogUtils.normalizePostgresFamilyTablePathQuery(
                        sqlQuery, tablePath, new PostgresDialect());

        Assertions.assertEquals(sqlQuery, normalizedQuery);
    }

    /**
     * Verify that table_path + query metadata lookup uses the table_path database connection.
     *
     * <p>This is the regression that still hit pg/openGauss/highgo after Web stopped emitting
     * database.schema.table SQL: metadata derivation still prepared the query on the catalog's
     * default database unless JdbcCatalogUtils routed the query through getTable(tablePath, query).
     */
    @Test
    public void testGetCatalogTableUsesTablePathAwareQueryMetadataLookup() throws Exception {
        JdbcSourceTableConfig tableConfig =
                JdbcSourceTableConfig.builder()
                        .tablePath("qa_sink.public.BATCH_SAVEMODE_1")
                        .query(
                                "SELECT \"id\" FROM \"qa_sink\".\"public\".\"BATCH_SAVEMODE_1\" "
                                        + "WHERE \"id\" > 100")
                        .build();
        TablePath tablePath = TablePath.of("qa_sink", "public", "BATCH_SAVEMODE_1");
        String normalizedQuery =
                "SELECT \"id\" FROM \"public\".\"BATCH_SAVEMODE_1\" WHERE \"id\" > 100";
        PostgresDialect jdbcDialect = new PostgresDialect();
        AbstractJdbcCatalog jdbcCatalog = Mockito.mock(AbstractJdbcCatalog.class);
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("jdbc_catalog", "qa_sink", "public", "BATCH_SAVEMODE_1"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        Mockito.when(jdbcCatalog.getTableIgnoreUnSupportColumn(tablePath))
                .thenThrow(new RuntimeException("ignore physical table for this regression"));
        Mockito.when(jdbcCatalog.getTable(tablePath, normalizedQuery)).thenReturn(tableOfQuery);

        Method getCatalogTableMethod =
                JdbcCatalogUtils.class.getDeclaredMethod(
                        "getCatalogTable",
                        JdbcSourceTableConfig.class,
                        AbstractJdbcCatalog.class,
                        JdbcDialect.class);
        getCatalogTableMethod.setAccessible(true);

        CatalogTable actual =
                (CatalogTable)
                        getCatalogTableMethod.invoke(null, tableConfig, jdbcCatalog, jdbcDialect);

        Assertions.assertEquals(tableOfQuery.getTableId(), actual.getTableId());
        Assertions.assertEquals(tableOfQuery.getTableSchema(), actual.getTableSchema());
        Mockito.verify(jdbcCatalog).getTable(tablePath, normalizedQuery);
        Mockito.verify(jdbcCatalog, Mockito.never()).getTable(tableConfig.getQuery());
    }
}
