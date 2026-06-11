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

package org.apache.seatunnel.transform.tablerenamer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.exception.TransformCommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableRenamerTest {

    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "Table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
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
    public void testConvert() {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, 1L, 1L});
        inputRow.setTableId(DEFAULT_TABLE.getTablePath().getFullName());
        AlterTableAddColumnEvent inputEvent =
                AlterTableAddColumnEvent.add(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null));

        List<CatalogTable> inputCatalogTable = Arrays.asList(DEFAULT_TABLE);
        TableRenamerConfig config = new TableRenamerConfig().setConvertCase(ConvertCase.LOWER);
        TableRenamerTransform transform = new TableRenamerTransform(inputCatalogTable, config);
        List<CatalogTable> outputCatalogTable = transform.getProducedCatalogTables();
        SeaTunnelRow outputRow = transform.map(inputRow);
        SchemaChangeEvent outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.table-x",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.table-x", outputRow.getTableId());
        Assertions.assertEquals("database-x.table-x", outputEvent.tablePath().getFullName());

        config = new TableRenamerConfig().setConvertCase(ConvertCase.UPPER);
        transform = new TableRenamerTransform(inputCatalogTable, config);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.TABLE-X",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.TABLE-X", outputRow.getTableId());
        Assertions.assertEquals("database-x.TABLE-X", outputEvent.tablePath().getFullName());

        config = new TableRenamerConfig().setPrefix("user-").setSuffix("-table");
        transform = new TableRenamerTransform(inputCatalogTable, config);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.user-Table-x-table",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.user-Table-x-table", outputRow.getTableId());
        Assertions.assertEquals(
                "database-x.user-Table-x-table", outputEvent.tablePath().getFullName());

        config =
                new TableRenamerConfig()
                        .setReplacementsWithRegex(
                                Arrays.asList(
                                        new TableRenamerConfig.ReplacementsWithRegex("Table", "t1"),
                                        new TableRenamerConfig.ReplacementsWithRegex(
                                                "Table", "t2")));
        transform = new TableRenamerTransform(inputCatalogTable, config);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.t2-x",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.t2-x", outputRow.getTableId());
        Assertions.assertEquals("database-x.t2-x", outputEvent.tablePath().getFullName());

        config =
                new TableRenamerConfig()
                        .setPrefix("user-")
                        .setSuffix("-table")
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify("Table-x", "aaa")));
        transform = new TableRenamerTransform(inputCatalogTable, config);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.aaa",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.aaa", outputRow.getTableId());
        Assertions.assertEquals("database-x.aaa", outputEvent.tablePath().getFullName());
    }

    @Test
    public void testConvertWithSchemaSplit() {
        List<CatalogTable> inputCatalogTable = Arrays.asList(DEFAULT_TABLE);
        TableRenamerConfig config =
                new TableRenamerConfig()
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify(
                                                "Table-x", "new_schema.new_table")));
        TableRenamerTransform transform = new TableRenamerTransform(inputCatalogTable, config);
        List<CatalogTable> outputCatalogTable = transform.getProducedCatalogTables();

        Assertions.assertEquals(
                "database-x.new_schema.new_table",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals(
                "new_schema", outputCatalogTable.get(0).getTableId().getSchemaName());
        Assertions.assertEquals("new_table", outputCatalogTable.get(0).getTableId().getTableName());
    }

    @Test
    public void testConvertWithDatabaseScopedSpecificRules() {
        CatalogTable otherDatabaseTable = createTable("database-y", null, "Table-x");
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, 1L, 1L});
        inputRow.setTableId(otherDatabaseTable.getTablePath().getFullName());
        AlterTableAddColumnEvent inputEvent =
                AlterTableAddColumnEvent.add(
                        otherDatabaseTable.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null));

        TableRenamerConfig config =
                new TableRenamerConfig()
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify(
                                                "database-x", "Table-x", "table_x_a"),
                                        new TableRenamerConfig.SpecificModify(
                                                "database-y", "Table-x", "table_x_b")));
        TableRenamerTransform transform =
                new TableRenamerTransform(Arrays.asList(DEFAULT_TABLE, otherDatabaseTable), config);
        List<CatalogTable> outputCatalogTable = transform.getProducedCatalogTables();
        SeaTunnelRow outputRow = transform.map(inputRow);
        SchemaChangeEvent outputEvent = transform.mapSchemaChangeEvent(inputEvent);

        Assertions.assertEquals(
                "database-x.table_x_a",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals(
                "database-y.table_x_b",
                outputCatalogTable.get(1).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-y.table_x_b", outputRow.getTableId());
        Assertions.assertEquals("database-y.table_x_b", outputEvent.tablePath().getFullName());
    }

    @Test
    public void testConvertWithDatabaseScopedLeafTableMatching() {
        CatalogTable schemaTable = createTable("database-x", "public", "Table-x");
        TableRenamerConfig config =
                new TableRenamerConfig()
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify(
                                                "database-x", "Table-x", "renamed_table")));
        TableRenamerTransform transform =
                new TableRenamerTransform(Arrays.asList(schemaTable), config);
        List<CatalogTable> outputCatalogTable = transform.getProducedCatalogTables();

        Assertions.assertEquals(
                "database-x.public.renamed_table",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("public", outputCatalogTable.get(0).getTableId().getSchemaName());
        Assertions.assertEquals(
                "renamed_table", outputCatalogTable.get(0).getTableId().getTableName());
    }

    @Test
    public void testDuplicateDatabaseScopedSpecificRule() {
        TableRenamerConfig config =
                new TableRenamerConfig()
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify(
                                                "database-x", "Table-x", "table_x_a"),
                                        new TableRenamerConfig.SpecificModify(
                                                "database-x", "Table-x", "table_x_b")));
        try {
            new TableRenamerTransform(Arrays.asList(DEFAULT_TABLE), config);
            Assertions.fail("Should throw exception");
        } catch (TransformException e) {
            if (!TransformCommonErrorCode.CONFIG_VALIDATION_FAILED.equals(
                    e.getSeaTunnelErrorCode())) {
                Assertions.fail(e);
            }
        } catch (Throwable e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testTableNotFound() {
        List<CatalogTable> inputCatalogTable = Arrays.asList(DEFAULT_TABLE);
        TableRenamerConfig config =
                new TableRenamerConfig()
                        .setSpecific(
                                Arrays.asList(
                                        new TableRenamerConfig.SpecificModify("Table-x", "a"),
                                        new TableRenamerConfig.SpecificModify("Table-1", "b")));
        TableRenamerTransform transform = new TableRenamerTransform(inputCatalogTable, config);
        try {
            transform.getProducedCatalogTables();
            Assertions.fail("Should throw exception");
        } catch (TransformException e) {
            if (!TransformCommonErrorCode.GET_CATALOG_TABLE_WITH_NOT_EXIST_TABLES_ERROR.equals(
                    e.getSeaTunnelErrorCode())) {
                Assertions.fail(e);
            }
            List<String> notExistTables = e.getParamsValueAs("tables");
            Assertions.assertIterableEquals(Collections.singletonList("Table-1"), notExistTables);
        } catch (Throwable e) {
            Assertions.fail(e);
        }
    }

    /** Copies the default schema into a different table identifier for scope-specific tests. */
    private CatalogTable createTable(String databaseName, String schemaName, String tableName) {
        return CatalogTable.of(
                TableIdentifier.of(
                        DEFAULT_TABLE.getTableId().getCatalogName(),
                        databaseName,
                        schemaName,
                        tableName),
                DEFAULT_TABLE);
    }
}
