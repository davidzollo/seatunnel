/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;

/**
 * Verifies the Redshift catalog keeps the existing pg_catalog path and can also consume
 * svv_columns-shaped metadata for late-binding views.
 */
public class RedshiftCatalogSqlTest {

    /**
     * The late-binding fallback must stay dormant for normal tables and activate only when the
     * pg_catalog path returns no rows.
     */
    @Test
    void testGetSelectColumnsSqlContainsLateBindingFallback() {
        RedshiftCatalog catalog = createCatalog();
        String sql = catalog.getSelectColumnsSql(TablePath.of("analytics", "public", "sales_v"));

        Assertions.assertTrue(sql.contains("WITH pg_columns AS"));
        Assertions.assertTrue(sql.contains("FROM svv_columns"));
        Assertions.assertTrue(sql.contains("WHERE NOT EXISTS (SELECT 1 FROM pg_columns)"));
        Assertions.assertTrue(sql.contains("table_catalog = 'analytics'"));
        Assertions.assertTrue(sql.contains("table_schema = 'public'"));
        Assertions.assertTrue(sql.contains("table_name = 'sales_v'"));
    }

    /**
     * svv_columns reports type names through the data_type field, so buildColumn must still map
     * them to the same SeaTunnel column definition used by the existing pg_catalog path.
     */
    @Test
    void testBuildColumnSupportsSvvColumnsMetadataShape() throws Exception {
        RedshiftCatalog catalog = createCatalog();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString("column_name")).thenReturn("customer_name");
        Mockito.when(resultSet.getString("type_name")).thenReturn("character varying");
        Mockito.when(resultSet.getString("full_type_name")).thenReturn("character varying");
        Mockito.when(resultSet.getLong("column_length")).thenReturn(128L);
        Mockito.when(resultSet.getInt("column_scale")).thenReturn(0);
        Mockito.when(resultSet.getString("column_comment")).thenReturn("customer name");
        Mockito.when(resultSet.getObject("default_value")).thenReturn(null);
        Mockito.when(resultSet.getString("is_nullable")).thenReturn("YES");

        Column column = catalog.buildColumn(resultSet);

        Assertions.assertEquals("customer_name", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals("CHARACTER VARYING(128)", column.getSourceType());
        Assertions.assertEquals("customer name", column.getComment());
        Assertions.assertTrue(column.isNullable());
    }

    /**
     * Decimal precision and scale from svv_columns must survive the fallback mapping, otherwise the
     * Web side will still see the column as incompatible.
     */
    @Test
    void testBuildColumnSupportsSvvColumnsNumericShape() throws Exception {
        RedshiftCatalog catalog = createCatalog();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString("column_name")).thenReturn("order_amount");
        Mockito.when(resultSet.getString("type_name")).thenReturn("numeric");
        Mockito.when(resultSet.getString("full_type_name")).thenReturn("numeric");
        Mockito.when(resultSet.getLong("column_length")).thenReturn(18L);
        Mockito.when(resultSet.getInt("column_scale")).thenReturn(4);
        Mockito.when(resultSet.getString("column_comment")).thenReturn(null);
        Mockito.when(resultSet.getObject("default_value")).thenReturn(null);
        Mockito.when(resultSet.getString("is_nullable")).thenReturn("NO");

        Column column = catalog.buildColumn(resultSet);

        Assertions.assertEquals("order_amount", column.getName());
        Assertions.assertEquals(new DecimalType(18, 4), column.getDataType());
        Assertions.assertEquals("NUMERIC(18,4)", column.getSourceType());
        Assertions.assertFalse(column.isNullable());
    }

    /** Builds a Redshift catalog instance without opening a live database connection. */
    private RedshiftCatalog createCatalog() {
        return new RedshiftCatalog(
                "test",
                "user",
                "pwd",
                JdbcUrlUtil.getUrlInfo("jdbc:redshift://localhost:5439/dev"),
                null);
    }
}
