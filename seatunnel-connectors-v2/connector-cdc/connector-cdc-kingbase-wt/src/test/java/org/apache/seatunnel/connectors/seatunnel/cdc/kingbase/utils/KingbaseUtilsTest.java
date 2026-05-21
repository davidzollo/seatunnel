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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.lang.reflect.Method;
import java.util.Optional;

public class KingbaseUtilsTest {

    @Test
    public void testSplitScanQueryUsesSchemaAndTableOnly() {
        String splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("sales.public.orders"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false,
                        null,
                        false);

        Assertions.assertEquals(
                "SELECT * FROM \"public\".\"orders\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitScanSQL);
    }

    @Test
    public void testSplitScanQueryUsesHashForStringSplitKey() {
        String splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("sales.public.orders"),
                        new SeaTunnelRowType(
                                new String[] {"name"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        false,
                        false,
                        new Object[] {10},
                        false);

        Assertions.assertEquals(
                "SELECT * FROM \"public\".\"orders\" WHERE (ABS(HASHTEXT(\"name\")) % 10) = ?",
                splitScanSQL);
    }

    @Test
    public void testBuildSelectWithRowLimitsUsesLimitClause() throws Exception {
        String limitedSql =
                (String)
                        invokeSqlBuilder(
                                "buildSelectWithRowLimits",
                                new Class<?>[] {
                                    TableId.class,
                                    int.class,
                                    String.class,
                                    Optional.class,
                                    Optional.class
                                },
                                TableId.parse("sales.public.orders"),
                                5,
                                "*",
                                Optional.of("\"id\" >= 10"),
                                Optional.of("\"id\""));

        Assertions.assertEquals(
                "SELECT * FROM \"public\".\"orders\" WHERE \"id\" >= 10 ORDER BY \"id\" LIMIT 5",
                limitedSql);
    }

    @Test
    public void testBuildSelectWithBoundaryRowLimitsUsesLimitClause() throws Exception {
        String limitedBoundarySql =
                (String)
                        invokeSqlBuilder(
                                "buildSelectWithBoundaryRowLimits",
                                new Class<?>[] {
                                    TableId.class,
                                    int.class,
                                    String.class,
                                    String.class,
                                    Optional.class,
                                    String.class
                                },
                                TableId.parse("sales.public.orders"),
                                5,
                                "\"id\"",
                                "MAX(id)",
                                Optional.of("\"id\" >= 10"),
                                "\"id\"");

        Assertions.assertEquals(
                "SELECT MAX(id) FROM (SELECT \"id\" FROM \"public\".\"orders\" WHERE \"id\" >= 10 ORDER BY \"id\" LIMIT 5) T",
                limitedBoundarySql);
    }

    private Object invokeSqlBuilder(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = KingbaseUtils.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }
}
