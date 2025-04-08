/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AdaptSinkTest {
    private static final CatalogTable INPUT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("default", TablePath.DEFAULT),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.builder()
                                            .name("id")
                                            .dataType(BasicType.LONG_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("name")
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("f1")
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("f2")
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null);
    private static final CatalogTable OUTPUT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("default", TablePath.DEFAULT),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.builder()
                                            .name("id")
                                            .dataType(BasicType.LONG_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("name")
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("f1")
                                            .dataType(BasicType.INT_TYPE)
                                            .build())
                            .column(
                                    PhysicalColumn.builder()
                                            .name("f3")
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null);

    @Test
    public void testAdaptSinkTableType() {
        AdaptSinkTransform transform =
                new AdaptSinkTransform(
                        Collections.singletonMap(TablePath.DEFAULT.toString(), INPUT_TABLE),
                        new AdaptSinkTransformConfig(
                                true,
                                false,
                                Collections.singletonMap(
                                        TablePath.DEFAULT.toString(), OUTPUT_TABLE)));

        transform.getProducedCatalogTable();
        Assertions.assertIterableEquals(
                Arrays.asList("id", "name", "f1", "f2"),
                transform.getProducedCatalogTable().getTableSchema().getColumnNames());
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                transform.getProducedCatalogTable().getTableSchema().getColumn("f1").getDataType());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        inputRow.setRowKind(RowKind.INSERT);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(inputRow.getTableId(), outputRow.getTableId());
        Assertions.assertEquals(inputRow.getRowKind(), outputRow.getRowKind());
        Assertions.assertEquals(inputRow.getArity(), outputRow.getArity());
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1, "2"}, outputRow.getFields());
    }

    @Test
    public void testAdaptSinkTableColumns() {
        AdaptSinkTransform transform =
                new AdaptSinkTransform(
                        Collections.singletonMap(TablePath.DEFAULT.toString(), INPUT_TABLE),
                        new AdaptSinkTransformConfig(
                                false,
                                true,
                                Collections.singletonMap(
                                        TablePath.DEFAULT.toString(), OUTPUT_TABLE)));

        transform.getProducedCatalogTable();
        Assertions.assertIterableEquals(
                Arrays.asList("id", "name", "f1"),
                transform.getProducedCatalogTable().getTableSchema().getColumnNames());
        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                transform.getProducedCatalogTable().getTableSchema().getColumn("f1").getDataType());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        inputRow.setRowKind(RowKind.INSERT);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(inputRow.getTableId(), outputRow.getTableId());
        Assertions.assertEquals(inputRow.getRowKind(), outputRow.getRowKind());
        Assertions.assertArrayEquals(new Object[] {1L, "n1", "1"}, outputRow.getFields());
    }

    @Test
    public void testAdaptSinkTable() {
        AdaptSinkTransform transform =
                new AdaptSinkTransform(
                        Collections.singletonMap(TablePath.DEFAULT.toString(), INPUT_TABLE),
                        new AdaptSinkTransformConfig(
                                true,
                                true,
                                Collections.singletonMap(
                                        TablePath.DEFAULT.toString(), OUTPUT_TABLE)));

        transform.getProducedCatalogTable();
        Assertions.assertIterableEquals(
                Arrays.asList("id", "name", "f1"),
                transform.getProducedCatalogTable().getTableSchema().getColumnNames());
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                transform.getProducedCatalogTable().getTableSchema().getColumn("f1").getDataType());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        inputRow.setRowKind(RowKind.INSERT);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(inputRow.getTableId(), outputRow.getTableId());
        Assertions.assertEquals(inputRow.getRowKind(), outputRow.getRowKind());
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1}, outputRow.getFields());
    }

    @Test
    public void testDdl() {
        AdaptSinkTransform transform =
                new AdaptSinkTransform(
                        createMap(TablePath.DEFAULT.toString(), INPUT_TABLE),
                        new AdaptSinkTransformConfig(
                                true, true, createMap(TablePath.DEFAULT.toString(), OUTPUT_TABLE)));

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        inputRow.setRowKind(RowKind.INSERT);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(inputRow.getTableId(), outputRow.getTableId());
        Assertions.assertEquals(inputRow.getRowKind(), outputRow.getRowKind());
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1}, outputRow.getFields());

        AlterTableAddColumnEvent addColumnEvent =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        PhysicalColumn.builder()
                                .name("f4")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        addColumnEvent = (AlterTableAddColumnEvent) transform.mapSchemaChangeEvent(addColumnEvent);
        Assertions.assertNull(addColumnEvent);
        inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2", "4"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        outputRow = transform.map(inputRow);
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1}, outputRow.getFields());

        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        PhysicalColumn.builder()
                                .name("f1")
                                .dataType(BasicType.BOOLEAN_TYPE)
                                .build());
        modifyColumnEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        Assertions.assertNull(modifyColumnEvent);
        inputRow = new SeaTunnelRow(new Object[] {1L, "n1", true, "2", "4"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        outputRow = transform.map(inputRow);
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1}, outputRow.getFields());

        AlterTableChangeColumnEvent changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        "f1",
                        PhysicalColumn.builder()
                                .name("f1_1")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        changeColumnEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        Assertions.assertNotNull(changeColumnEvent);
        inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "1", "2", "4"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        outputRow = transform.map(inputRow);
        Assertions.assertArrayEquals(new Object[] {1L, "n1", 1}, outputRow.getFields());

        AlterTableDropColumnEvent dropColumnEvent =
                new AlterTableDropColumnEvent(
                        TableIdentifier.of("default", TablePath.DEFAULT), "f1_1");
        dropColumnEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);
        Assertions.assertNotNull(dropColumnEvent);
        inputRow = new SeaTunnelRow(new Object[] {1L, "n1", "2", "4"});
        inputRow.setTableId(TablePath.DEFAULT.toString());
        outputRow = transform.map(inputRow);
        Assertions.assertArrayEquals(new Object[] {1L, "n1"}, outputRow.getFields());

        modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        PhysicalColumn.builder().name("f2").dataType(BasicType.INT_TYPE).build());
        modifyColumnEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        Assertions.assertNull(modifyColumnEvent);

        changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        "f2",
                        PhysicalColumn.builder()
                                .name("f2_1")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        changeColumnEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        Assertions.assertNull(changeColumnEvent);

        dropColumnEvent =
                new AlterTableDropColumnEvent(
                        TableIdentifier.of("default", TablePath.DEFAULT), "f2_1");
        dropColumnEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);
        Assertions.assertNull(dropColumnEvent);
    }

    static Map<String, CatalogTable> createMap(String key, CatalogTable table) {
        Map<String, CatalogTable> map = new HashMap<>();
        map.put(key, table);
        return map;
    }
}
