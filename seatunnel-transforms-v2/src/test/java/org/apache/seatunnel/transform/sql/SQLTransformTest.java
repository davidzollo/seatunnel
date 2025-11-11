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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class SQLTransformTest {

    private static final String TEST_NAME = "test";
    private static final String TIMESTAMP_FILEDNAME = "create_time";
    private static final String[] FILED_NAMES =
            new String[] {"id", "name", "age", TIMESTAMP_FILEDNAME};
    private static final String GENERATE_PARTITION_KEY = "dt";
    private static final SQLTransformConfig sqlTransformConfig =
            new SQLTransformConfig() {
                {
                    setQuery(
                            "select *,FORMATDATETIME(create_time,'yyyy-MM-dd HH:mm') as dt from dual");
                    setEngineType(SQLEngineFactory.EngineType.ZETA);
                }
            };
    private static final ReadonlyConfig READONLY_CONFIG =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                        }
                    });

    @Test
    public void testScaleSupport() {
        SQLTransform sqlTransform =
                new SQLTransform(sqlTransformConfig, READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (column.getName().equals(TIMESTAMP_FILEDNAME)) {
                                Assertions.assertEquals(9, column.getScale());
                            } else if (column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertTrue(Objects.isNull(column.getScale()));
                            } else {
                                Assertions.assertEquals(3, column.getColumnLength());
                            }
                        });
    }

    @Test
    public void testQueryWithAnyTable() {
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery("select * from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        Assertions.assertEquals(4, tableSchema.getColumns().size());
    }

    @Test
    public void testNotLoseSourceTypeAndOptions() {
        SQLTransform sqlTransform =
                new SQLTransform(sqlTransformConfig, READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (!column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertEquals(
                                        "source_" + column.getDataType(), column.getSourceType());
                                Assertions.assertEquals(
                                        "testInSQL", column.getOptions().get("context"));
                            }
                        });
    }

    private CatalogTable getCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        FILED_NAMES,
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Integer scale = null;
            Long columnLength = null;
            if (rowType.getFieldName(i).equals(TIMESTAMP_FILEDNAME)) {
                scale = 9;
            } else {
                columnLength = 3L;
            }
            PhysicalColumn column =
                    new PhysicalColumn(
                            rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            columnLength,
                            scale,
                            true,
                            null,
                            null,
                            "source_" + rowType.getFieldType(i),
                            new HashMap<String, Object>() {
                                {
                                    put("context", "testInSQL");
                                }
                            });
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of(TEST_NAME, TEST_NAME, null, TEST_NAME),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It has column information.");
    }

    @Test
    public void tesCaseWhenClausesWithBooleanField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "bool"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE
                                }));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery(
                                        "select `id`, `bool`, case when bool then 1 else 2 end as bool_1 from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), true})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(true, result.get(0).getField(1));
        Assertions.assertEquals(1, result.get(0).getField(2));

        result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), false})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(false, result.get(0).getField(1));
        Assertions.assertEquals(2, result.get(0).getField(2));
    }

    @Test
    public void tesCaseWhenBooleanClausesWithField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery(
                                        "select `id`, `int`, (case when `int` = 1 then true else false end) as bool_1 , `string`, (case when `string` = 'true' then true else false end) as bool_2 from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 1, "true"})));

        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(1, result.get(0).getField(1));
        Assertions.assertEquals(true, result.get(0).getField(2));
        Assertions.assertEquals("true", result.get(0).getField(3));
        Assertions.assertEquals(true, result.get(0).getField(4));

        result =
                Collections.singletonList(
                        sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 0, "false"})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(0, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
        Assertions.assertEquals("false", result.get(0).getField(3));
        Assertions.assertEquals(false, result.get(0).getField(4));
    }

    @Test
    public void tesCastBooleanClausesWithField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery(
                                        "select `id`, `int`, cast(`int` as boolean) as bool_1 , `string`, cast(`string` as boolean) as bool_2 from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 1, "true"})));

        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(1, result.get(0).getField(1));
        Assertions.assertEquals(true, result.get(0).getField(2));
        Assertions.assertEquals("true", result.get(0).getField(3));
        Assertions.assertEquals(true, result.get(0).getField(4));

        result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 0, "false"})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(0, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
        Assertions.assertEquals("false", result.get(0).getField(3));
        Assertions.assertEquals(false, result.get(0).getField(4));

        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 3, "false"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST AS Boolean: 3",
                                e.getMessage());
                        throw e;
                    }
                });

        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 0, "false333"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST AS Boolean: false333",
                                e.getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void tesBooleanField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery("select `id`, true as bool_1, false as bool_2 from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 1, "true"})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(true, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
    }

    @Test
    public void testTrimWithCastExpression() {
        String tableName = "test";
        String[] fields = new String[] {"id", "code"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE}));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery(
                                        "select `id`, trim(cast(`code` as string)) as trimmed_code from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 12345})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("12345", result.get(0).getField(1));
    }

    @Test
    public void testCastDecimalToInteger() {
        String tableName = "test";
        String[] fields = new String[] {"id", "price", "amount"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.DOUBLE_TYPE, BasicType.STRING_TYPE
                                }));
        SQLTransform sqlTransform =
                new SQLTransform(
                        new SQLTransformConfig() {
                            {
                                setQuery(
                                        "select `id`, cast(`price` as int) as price_int, cast(`amount` as bigint) as amount_long from dual");
                                setEngineType(SQLEngineFactory.EngineType.ZETA);
                            }
                        },
                        READONLY_CONFIG,
                        table);

        // Test casting double to int
        List<SeaTunnelRow> result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 313, "999"})));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(313, result.get(0).getField(1));
        Assertions.assertEquals(999L, result.get(0).getField(2));

        // Test casting string decimal to int
        result =
                Collections.singletonList(
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(2), 100, "12345"})));
        Assertions.assertEquals(2, result.get(0).getField(0));
        Assertions.assertEquals(100, result.get(0).getField(1));
        Assertions.assertEquals(12345L, result.get(0).getField(2));
    }
}
