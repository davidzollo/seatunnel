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

package org.apache.seatunnel.transform.remover;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.fieldremover.FieldRemoverConfig;
import org.apache.seatunnel.transform.fieldremover.FieldRemoverTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class RemoverTest {

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
    void testProduceNewCatalogTable() {
        FieldRemoverConfig config = new FieldRemoverConfig();
        config.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-x", Arrays.asList("f1", "f2"));
                    }
                });
        FieldRemoverTransform transform =
                new FieldRemoverTransform(Collections.singletonList(DEFAULT_TABLE), config);
        List<CatalogTable> newCatalogTables = transform.getProducedCatalogTables();
        Assertions.assertEquals(1, newCatalogTables.size());
        Assertions.assertEquals(1, newCatalogTables.get(0).getTableSchema().getColumns().size());
        Assertions.assertEquals(
                "f3", newCatalogTables.get(0).getTableSchema().getColumns().get(0).getName());

        FieldRemoverConfig config2 = new FieldRemoverConfig();
        config2.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-notfound", Arrays.asList("f1", "f2"));
                    }
                });
        FieldRemoverTransform transform2 =
                new FieldRemoverTransform(Collections.singletonList(DEFAULT_TABLE), config2);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class, transform2::getProducedCatalogTables);
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-06], ErrorDescription:[The 'FieldRemover' upstream schema not exist tables '[\"database-x.table-notfound\"]']",
                exception.getMessage());

        FieldRemoverConfig config3 = new FieldRemoverConfig();
        config3.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-x", Arrays.asList("f4", "f5"));
                    }
                });
        FieldRemoverTransform transform3 =
                new FieldRemoverTransform(Collections.singletonList(DEFAULT_TABLE), config3);
        TransformException exception2 =
                Assertions.assertThrows(
                        TransformException.class, transform3::getProducedCatalogTables);
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-05], ErrorDescription:[The 'FieldRemover' upstream schema not exist fields: '{\"database-x.table-x\":[\"f4\",\"f5\"]}']",
                exception2.getMessage());

        FieldRemoverConfig config4 = new FieldRemoverConfig();
        config4.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-x", Arrays.asList("f4", "f5"));
                        put("database-x.table-notfound", Arrays.asList("f4", "f5"));
                    }
                });
        FieldRemoverTransform transform4 =
                new FieldRemoverTransform(Collections.singletonList(DEFAULT_TABLE), config4);
        TransformException exception3 =
                Assertions.assertThrows(
                        TransformException.class, transform4::getProducedCatalogTables);
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-07], ErrorDescription:[The 'FieldRemover' upstream schema not exist table '[\"database-x.table-notfound\"]'，upstream schema not exist fields: '{\"database-x.table-x\":[\"f4\",\"f5\"]}']",
                exception3.getMessage());
    }

    @Test
    void testMapNewSeaTunnelRow() {
        FieldRemoverConfig config = new FieldRemoverConfig();
        config.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-x", Arrays.asList("f1", "f2"));
                    }
                });
        FieldRemoverTransform transform =
                new FieldRemoverTransform(Collections.singletonList(DEFAULT_TABLE), config);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "a", "b"});
        row.setTableId("database-x.table-x");
        SeaTunnelRow newRow = new SeaTunnelRow(new Object[] {"b"});
        newRow.setTableId("database-x.table-x");
        Assertions.assertEquals(newRow, transform.map(row));
    }

    @Test
    void testTableEventChange() {
        FieldRemoverConfig config = new FieldRemoverConfig();
        config.setRemovedFields(
                new LinkedHashMap<String, List<String>>() {
                    {
                        put("database-x.table-x", Collections.singletonList("f2"));
                    }
                });
        FieldRemoverTransform transform =
                new FieldRemoverTransform(
                        Arrays.asList(
                                DEFAULT_TABLE,
                                CatalogTable.of(
                                        TableIdentifier.of("catalog", TablePath.of("test.test2")),
                                        DEFAULT_TABLE)),
                        config);
        AlterTableAddColumnEvent alterTableAddColumnEvent =
                new AlterTableAddColumnEvent(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of(
                                "f4",
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
                                null),
                        true,
                        null);
        Assertions.assertEquals(
                alterTableAddColumnEvent, transform.mapSchemaChangeEvent(alterTableAddColumnEvent));

        AlterTableAddColumnEvent alterTableAddColumnEvent2 =
                new AlterTableAddColumnEvent(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of(
                                "f2",
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
                                null),
                        true,
                        null);
        Assertions.assertNull(transform.mapSchemaChangeEvent(alterTableAddColumnEvent2));

        AlterTableNameEvent alterTableNameEvent =
                new AlterTableNameEvent(
                        DEFAULT_TABLE.getTableId(),
                        TableIdentifier.of(
                                DEFAULT_TABLE.getCatalogName(), TablePath.of("test", "test3")));
        transform.mapSchemaChangeEvent(alterTableNameEvent);
        AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                new AlterTableChangeColumnEvent(
                        TableIdentifier.of(null, TablePath.of("test.test3")),
                        "f2",
                        PhysicalColumn.of(
                                "f3_xx",
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
                                null),
                        false,
                        "f2");
        Assertions.assertNull(transform.mapSchemaChangeEvent(alterTableChangeColumnEvent));

        AlterTableColumnsEvent alterTableColumnsEvent =
                new AlterTableColumnsEvent(
                        DEFAULT_TABLE.getTableId(),
                        Collections.singletonList(
                                new AlterTableChangeColumnEvent(
                                        TableIdentifier.of(null, TablePath.of("test.test3")),
                                        "f2",
                                        PhysicalColumn.of(
                                                "f3_xx",
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
                                                null),
                                        false,
                                        "f2")));
        Assertions.assertNull(transform.mapSchemaChangeEvent(alterTableColumnsEvent));
    }
}
