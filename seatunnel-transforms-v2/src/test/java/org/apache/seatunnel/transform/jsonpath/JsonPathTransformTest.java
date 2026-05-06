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
package org.apache.seatunnel.transform.jsonpath;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class JsonPathTransformTest {

    @Test
    void shouldKeepRemainingFieldsAlignedWhenDeleteSourceField() {
        CatalogTable inputTable =
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
                                                .name("graph")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .column(
                                        PhysicalColumn.builder()
                                                .name("schedule")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .column(
                                        PhysicalColumn.builder()
                                                .name("inputs")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .column(
                                        PhysicalColumn.builder()
                                                .name("config")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .column(
                                        PhysicalColumn.builder()
                                                .name("creator")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        JsonPathTransform transform =
                new JsonPathTransform(
                        new JsonPathTransformConfig(
                                Collections.singletonList(
                                        new ColumnConfig(
                                                "$['schedule']",
                                                "schedule",
                                                true,
                                                "schedule_value",
                                                PhysicalColumn.builder()
                                                        .name("schedule_value")
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .columnLength(1020L)
                                                        .defaultValue("")
                                                        .build(),
                                                ErrorHandleWay.FAIL)),
                                null),
                        inputTable);

        Assertions.assertIterableEquals(
                Arrays.asList("id", "graph", "inputs", "config", "creator", "schedule_value"),
                transform.getProducedCatalogTable().getTableSchema().getColumnNames());

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L,
                            "{\"graph\":\"graph\"}",
                            "{\"schedule\":\"schedule\"}",
                            "{\"inputs\":\"inputs\"}",
                            "{\"config\":\"config\"}",
                            "{\"creator\":\"creator\"}"
                        });
        inputRow.setTableId(TablePath.DEFAULT.toString());
        inputRow.setRowKind(RowKind.INSERT);

        SeaTunnelRow outputRow = transform.map(inputRow);

        Assertions.assertEquals(inputRow.getTableId(), outputRow.getTableId());
        Assertions.assertEquals(inputRow.getRowKind(), outputRow.getRowKind());
        Assertions.assertArrayEquals(
                new Object[] {
                    1L,
                    "{\"graph\":\"graph\"}",
                    "{\"inputs\":\"inputs\"}",
                    "{\"config\":\"config\"}",
                    "{\"creator\":\"creator\"}",
                    "schedule"
                },
                outputRow.getFields());
    }
}
