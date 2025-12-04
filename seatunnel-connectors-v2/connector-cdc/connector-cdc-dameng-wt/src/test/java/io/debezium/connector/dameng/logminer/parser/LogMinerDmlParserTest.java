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

package io.debezium.connector.dameng.logminer.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class LogMinerDmlParserTest {

    private final LogMinerDmlParser parser = new LogMinerDmlParser();

    private Table createTestTable() {
        return Table.editor()
                .tableId(TableId.parse("CS_TSZF.CS_TSZF"))
                .addColumn(
                        Column.editor()
                                .name("ID")
                                .type("NUMBER")
                                .jdbcType(java.sql.Types.NUMERIC)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("NAME")
                                .type("VARCHAR2")
                                .jdbcType(java.sql.Types.VARCHAR)
                                .create())
                .create();
    }

    /**
     * Test parsing INSERT statement with escaped single quote (O''Reilly -> O'Reilly).
     *
     * <p>This test case reproduces GitHub Issue #3913.
     */
    @Test
    public void testParseInsertWithEscapedSingleQuote() {
        String sql = "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (5,'O''Reilly')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId123");

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.getOperation()); // INSERT operation
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals(2, newValues.length);
        Assertions.assertEquals("5", newValues[0].toString());
        Assertions.assertEquals("O'Reilly", newValues[1]); // Should be unescaped
    }

    /**
     * Test parsing INSERT statement with SQL injection-like escaped quotes (User'' OR 1=1; -- ->
     * User' OR 1=1; --).
     *
     * <p>This test case reproduces GitHub Issue #3913.
     */
    @Test
    public void testParseInsertWithSQLInjectionLikeEscapedQuote() {
        String sql =
                "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (14,'User'' OR 1=1; --')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId456");

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.getOperation()); // INSERT operation
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals(2, newValues.length);
        Assertions.assertEquals("14", newValues[0].toString());
        Assertions.assertEquals("User' OR 1=1; --", newValues[1]); // Should be unescaped
    }

    /** Test parsing UPDATE statement with escaped single quotes. */
    @Test
    public void testParseUpdateWithEscapedSingleQuote() {
        String sql =
                "update \"CS_TSZF\".\"CS_TSZF\" set \"NAME\" = 'O''Reilly' where \"ID\" = 5 and \"NAME\" = 'Test'";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId789");

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(3, entry.getOperation()); // UPDATE operation (RowMapper.UPDATE = 3)
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals(2, newValues.length);
        Assertions.assertEquals("O'Reilly", newValues[1]); // Should be unescaped
    }

    /** Test parsing DELETE statement with escaped single quotes in WHERE clause. */
    @Test
    public void testParseDeleteWithEscapedSingleQuote() {
        String sql =
                "delete from \"CS_TSZF\".\"CS_TSZF\" where \"ID\" = 5 and \"NAME\" = 'O''Reilly'";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId999");

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(2, entry.getOperation()); // DELETE operation (RowMapper.DELETE = 2)
        Object[] oldValues = entry.getOldValues();
        Assertions.assertEquals(2, oldValues.length);
        Assertions.assertEquals("O'Reilly", oldValues[1]); // Should be unescaped
    }

    /** Test parsing INSERT with multiple escaped quotes in one string. */
    @Test
    public void testParseInsertWithMultipleEscapedQuotes() {
        String sql =
                "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (20,'It''s a ''test'' string')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId111");

        Assertions.assertNotNull(entry);
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals("It's a 'test' string", newValues[1]); // All quotes unescaped
    }

    /** Test parsing INSERT with normal string (no escaped quotes). */
    @Test
    public void testParseInsertWithNormalString() {
        String sql =
                "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (1,'Normal String')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId222");

        Assertions.assertNotNull(entry);
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals("Normal String", newValues[1]); // No change
    }

    /** Test parsing INSERT with empty string. */
    @Test
    public void testParseInsertWithEmptyString() {
        String sql = "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (2,'')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId333");

        Assertions.assertNotNull(entry);
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals("", newValues[1]); // Empty string
    }

    /** Test parsing INSERT with string containing only escaped quotes. */
    @Test
    public void testParseInsertWithOnlyEscapedQuotes() {
        String sql = "insert into \"CS_TSZF\".\"CS_TSZF\"(\"ID\",\"NAME\") values (3,'''')";
        Table table = createTestTable();

        LogMinerDmlEntry entry = parser.parse(sql, table, "txId444");

        Assertions.assertNotNull(entry);
        Object[] newValues = entry.getNewValues();
        Assertions.assertEquals("'", newValues[1]); // Single quote
    }
}
