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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;

import java.lang.reflect.Method;
import java.util.List;

public class ClickhouseProxyTest {

    /** Helper method to invoke the private method parseIndexExprToColumnNames via reflection. */
    @SuppressWarnings("unchecked")
    private List<String> parseIndexExpr(String expr) throws Exception {
        // Use Mockito to create a mock object without invoking the constructor.
        ClickhouseProxy proxy = Mockito.mock(ClickhouseProxy.class);

        Method method =
                ClickhouseProxy.class.getDeclaredMethod(
                        "parseIndexExprToColumnNames", String.class);
        method.setAccessible(true);
        return (List<String>) method.invoke(proxy, expr);
    }

    @Test
    public void testParseIndexExpr_HappyPaths() throws Exception {
        // Case 1: Simple column
        List<String> cols = parseIndexExpr("status");
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("status", cols.get(0));

        // Case 2: Simple function
        cols = parseIndexExpr("minmax(status)");
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("status", cols.get(0));

        // Case 3: Tuple function
        cols = parseIndexExpr("set(col1, col2)");
        Assertions.assertEquals(2, cols.size());
        Assertions.assertTrue(cols.contains("col1"));
        Assertions.assertTrue(cols.contains("col2"));
    }

    @Test
    public void testParseIndexExpr_QuotedIdentifiers() throws Exception {
        // Case: Quoted identifier
        List<String> cols = parseIndexExpr("`status`");

        // Expectation: Should extract "status"
        Assertions.assertEquals(1, cols.size(), "Should extract 1 column from quoted identifier");
        Assertions.assertEquals("status", cols.get(0));
    }

    @Test
    public void testParseIndexExpr_NestedFunctions() throws Exception {
        // Case: Nested functions
        List<String> cols = parseIndexExpr("bloom_filter(toLowCardinality(my_col))");

        // Expectation: Should extract "my_col"
        Assertions.assertEquals(
                1, cols.size(), "Should extract inner column from nested functions");
        Assertions.assertEquals("my_col", cols.get(0));
    }

    @Test
    public void testParseIndexExpr_Expressions() throws Exception {
        // Case: Arithmetic or other expressions
        // Note: This might be hard to support perfectly without a full parser, but let's assume we
        // want 'col1'
        List<String> cols = parseIndexExpr("col1 * 2");

        Assertions.assertFalse(cols.isEmpty(), "Should extract column from expression");
        Assertions.assertTrue(cols.contains("col1"));
    }

    @Test
    public void testParseIndexExpr_ComplexTuple() throws Exception {
        // Case: Tuple with nested function
        // Assuming the expression is something like "ngram(col1, lower(col2))" or similar
        List<String> cols = parseIndexExpr("ngram(col1, lower(col2))");

        Assertions.assertEquals(2, cols.size(), "Should extract 2 columns from complex function");
        Assertions.assertTrue(cols.contains("col1"));
        Assertions.assertTrue(cols.contains("col2"));
    }

    @Test
    public void testNormalizeShardHostFallbackToDatasourceHost() throws Exception {
        ClickHouseNode node =
                ClickHouseNode.builder()
                        .host("datasource01")
                        .port(ClickHouseProtocol.HTTP, 8123)
                        .database("default")
                        .build();
        ClickhouseProxy proxy = new ClickhouseProxy(node);
        Method method =
                ClickhouseProxy.class.getDeclaredMethod(
                        "normalizeShardHost", String.class, String.class);
        method.setAccessible(true);

        String host = (String) method.invoke(proxy, "localhost", "127.0.0.1");

        Assertions.assertEquals("datasource01", host);
    }
}
