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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verify SQL templates cast sql_variant values to NVARCHAR to avoid driver errors. */
public class SqlServerCatalogSqlTest {

    @Test
    void testSelectColumnsSqlTemplateContainsConvert() {
        String sql = String.format(SqlServerCatalog.SELECT_COLUMNS_SQL_TEMPLATE, "dbo", "");
        Assertions.assertTrue(sql.contains("CONVERT(NVARCHAR(4000), ext.value) AS comment"));
        Assertions.assertTrue(
                sql.contains("CONVERT(NVARCHAR(4000), def.definition) AS default_value"));
    }

    @Test
    void testGetSelectColumnsSqlByTableContainsConvert() {
        SqlServerCatalog catalog =
                new SqlServerCatalog(
                        "test",
                        "user",
                        "pwd",
                        SqlServerURLParser.parse("jdbc:sqlserver://localhost:1433;database=master"),
                        null);
        TablePath tp = TablePath.of("db", "dbo", "My Table$");
        String sql = catalog.getSelectColumnsSql(tp);
        Assertions.assertTrue(sql.contains("CONVERT(NVARCHAR(4000), ext.value) AS comment"));
        Assertions.assertTrue(
                sql.contains("CONVERT(NVARCHAR(4000), def.definition) AS default_value"));
    }

    @Test
    void testSelectTableCommentSqlContainsConvert() {
        Assertions.assertTrue(
                SqlServerCatalog.SELECT_TABLE_COMMENT_SQL.contains(
                        "CONVERT(NVARCHAR(4000), p.value)"));
    }

    @Test
    void testGetSelectColumnsSqlWithSingleQuotesEscaped() {
        SqlServerCatalog catalog =
                new SqlServerCatalog(
                        "test",
                        "user",
                        "pwd",
                        SqlServerURLParser.parse("jdbc:sqlserver://localhost:1433;database=master"),
                        null);
        TablePath tablePath = TablePath.of("test_db", "dbo", "Table'With'Quotes");
        String sql = catalog.getSelectColumnsSql(tablePath);

        // Check that single quotes in table name are properly escaped with double single quotes
        Assertions.assertTrue(sql.contains("tbl.name = N'Table''With''Quotes'"));
    }
}
