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

package org.apache.seatunnel.transform.mapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MapperTransformTest {

    static CatalogTable catalogTable;
    static Object[] values;
    static SeaTunnelRow inputRow;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "amount",
                                                new DecimalType(10, 2),
                                                10L,
                                                2,
                                                Boolean.FALSE,
                                                null,
                                                null,
                                                null,
                                                "DECIMAL(10,2)"))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values =
                new Object[] {
                    "1",
                    "value2",
                    LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                    LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                    "value5",
                    new BigDecimal("123.45")
                };
        inputRow = new SeaTunnelRow(values);
    }

    @Test
    public void testMapperTransformConfig() {

        List<MapperConfig.SpecificModify> specificModifies =
                Lists.newArrayList(
                        new MapperConfig.SpecificModify(
                                "default.default",
                                "schema.table",
                                null,
                                Lists.newArrayList(
                                        MapperConfig.Column.builder()
                                                .position(2)
                                                .inputName("key1")
                                                .outputName("id")
                                                .dataType(SqlType.INT)
                                                .dateFormat("")
                                                .length(10L)
                                                .scale(0)
                                                .nullable(false)
                                                .sinkType("INT")
                                                .defaultValue(null)
                                                .comment("ID")
                                                .sqlFunction(null)
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(1)
                                                .inputName("key2")
                                                .outputName("name")
                                                .dataType(SqlType.STRING)
                                                .dateFormat("")
                                                .length(255L)
                                                .scale(0)
                                                .nullable(true)
                                                .sinkType("VARCHAR(255)")
                                                .defaultValue("")
                                                .comment("Full user name")
                                                .sqlFunction("UPPER(key2)")
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(3)
                                                .inputName("key3")
                                                .outputName("time1")
                                                .dataType(SqlType.STRING)
                                                .dateFormat("yyyy_MM_dd_HH_mm_ss_SSSSSS")
                                                .length(null)
                                                .scale(null)
                                                .nullable(true)
                                                .sinkType("")
                                                .defaultValue(null)
                                                .comment("time1")
                                                .sqlFunction(null)
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(4)
                                                .inputName("key4")
                                                .outputName("time2")
                                                .dataType(SqlType.TIMESTAMP)
                                                .dateFormat("yyyy_MM_dd_HH_mm")
                                                .length(null)
                                                .scale(null)
                                                .nullable(true)
                                                .sinkType("")
                                                .defaultValue(null)
                                                .comment("time2")
                                                .sqlFunction(null)
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(5)
                                                .inputName("key5")
                                                .outputName("key5s")
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(6)
                                                .inputName("amount")
                                                .outputName("amount")
                                                .dataType(SqlType.DECIMAL)
                                                .length(20L)
                                                .scale(5)
                                                .nullable(false)
                                                .action(MapperConfig.Action.MODIFY)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(7)
                                                .outputName("new_col_1")
                                                .dataType(SqlType.STRING)
                                                .action(MapperConfig.Action.ADD)
                                                .build(),
                                        MapperConfig.Column.builder()
                                                .position(8)
                                                .outputName("new_col_2")
                                                .dataType(SqlType.STRING)
                                                .sqlFunction("UPPER(key2)")
                                                .action(MapperConfig.Action.ADD)
                                                .build()),
                                new MapperConfig.Primarykey(
                                        "pk_id",
                                        Lists.newArrayList(
                                                new MapperConfig.ReferenceColumn(
                                                        "id", ConstraintKey.ColumnSortType.ASC)),
                                        MapperConfig.Action.ADD),
                                Lists.newArrayList(
                                        new MapperConfig.Index(
                                                "idx_full_name",
                                                false,
                                                Lists.newArrayList(
                                                        new MapperConfig.ReferenceColumn(
                                                                "full_name",
                                                                ConstraintKey.ColumnSortType.ASC)),
                                                MapperConfig.Action.ADD)),
                                null,
                                new MapperConfig.Comment(
                                        "User information table, containing basic user data",
                                        MapperConfig.Action.ADD)));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(MapperConfig.SPECIFIC.key(), specificModifies);
                            }
                        });

        MapperTransform mapperTransform = new MapperTransform(config, catalogTable);
        Assertions.assertEquals(
                "default.schema.table",
                mapperTransform.transformTableIdentifier().toTablePath().getFullName());
        Assertions.assertIterableEquals(
                Arrays.asList(
                        "name",
                        "id",
                        "time1",
                        "time2",
                        "key5s",
                        "amount",
                        "new_col_1",
                        "new_col_2"),
                Arrays.asList(
                        Arrays.stream(mapperTransform.getOutputColumns())
                                .map(Column::getName)
                                .toArray(String[]::new)));
        Assertions.assertIterableEquals(
                Arrays.asList(
                        "VALUE2",
                        1,
                        "2000_10_29_10_29_11_111111",
                        LocalDateTime.of(2000, 10, 29, 10, 29, 0, 0),
                        "value5",
                        new BigDecimal("123.45"),
                        null,
                        "VALUE2"),
                Arrays.asList(
                        mapperTransform.getOutputFieldValues(new SeaTunnelRowAccessor(inputRow))));

        Column amountColumn =
                Arrays.stream(mapperTransform.getOutputColumns())
                        .filter(col -> col.getName().equals("amount"))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(amountColumn);
        Assertions.assertEquals(SqlType.DECIMAL, amountColumn.getDataType().getSqlType());
        Assertions.assertEquals(20L, amountColumn.getColumnLength());
        Assertions.assertEquals(5, amountColumn.getScale());
        Assertions.assertNull(amountColumn.getSourceType());
        Assertions.assertTrue(amountColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) amountColumn.getDataType();
        Assertions.assertEquals(20, decimalType.getPrecision());
        Assertions.assertEquals(5, decimalType.getScale());
    }
}
