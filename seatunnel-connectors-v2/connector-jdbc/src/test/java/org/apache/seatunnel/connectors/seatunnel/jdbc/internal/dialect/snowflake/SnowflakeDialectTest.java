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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for SnowflakeDialect to verify upsert statement generation and dialect functionality
 * after fixes.
 */
public class SnowflakeDialectTest {

    private final SnowflakeDialect dialect = new SnowflakeDialect();

    @Test
    public void testDialectName() {
        assertEquals(DatabaseIdentifier.SNOWFLAKE, dialect.dialectName());
    }

    @Test
    public void testUpsertStatementGeneration() {
        String database = "test_db";
        String tableName = "test_table";
        String[] fieldNames = {"id", "name", "value", "updated_time"};
        String[] uniqueKeyFields = {"id"};

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields, false);

        assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();

        // Verify the MERGE statement structure
        assertTrue(sql.contains("MERGE INTO \"test_db\".\"test_table\" AS target"));
        assertTrue(
                sql.contains(
                        "USING (SELECT ? AS \"id\", ? AS \"name\", ? AS \"value\", ? AS \"updated_time\") AS source"));
        assertTrue(sql.contains("ON target.\"id\" = source.\"id\""));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assertTrue(sql.contains("\"name\" = source.\"name\""));
        assertTrue(sql.contains("\"value\" = source.\"value\""));
        assertTrue(sql.contains("\"updated_time\" = source.\"updated_time\""));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT"));
        assertTrue(
                sql.contains(
                        "VALUES (source.\"id\", source.\"name\", source.\"value\", source.\"updated_time\")"));

        // Verify id field is not in UPDATE clause (as it's the unique key)
        String updateClause = sql.substring(sql.indexOf("UPDATE SET"));
        assertFalse(updateClause.contains("\"id\" = source.\"id\""));
    }

    @Test
    public void testUpsertStatementWithMultipleKeys() {
        String database = "test_db";
        String tableName = "test_table";
        String[] fieldNames = {"tenant_id", "user_id", "name", "value"};
        String[] uniqueKeyFields = {"tenant_id", "user_id"};

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields, false);

        assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();

        // Verify multiple key conditions
        assertTrue(
                sql.contains(
                        "ON target.\"tenant_id\" = source.\"tenant_id\" AND target.\"user_id\" = source.\"user_id\""));

        // Verify only non-key fields are updated
        assertTrue(sql.contains("\"name\" = source.\"name\""));
        assertTrue(sql.contains("\"value\" = source.\"value\""));
        String updateClause = sql.substring(sql.indexOf("UPDATE SET"));
        assertFalse(updateClause.contains("\"tenant_id\" = source.\"tenant_id\""));
        assertFalse(updateClause.contains("\"user_id\" = source.\"user_id\""));
    }

    @Test
    public void testUpsertStatementWithoutDatabase() {
        String tableName = "test_table";
        String[] fieldNames = {"id", "name", "value"};
        String[] uniqueKeyFields = {"id"};

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(null, tableName, fieldNames, uniqueKeyFields, false);

        assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();

        // Should use table name without database prefix
        assertTrue(sql.contains("MERGE INTO \"test_table\" AS target"));
    }

    @Test
    public void testUpsertStatementWithoutUniqueKeys() {
        String database = "test_db";
        String tableName = "test_table";
        String[] fieldNames = {"id", "name", "value"};
        String[] uniqueKeyFields = null;

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields, false);

        assertFalse(upsertStatement.isPresent());

        // Test with empty unique keys
        String[] emptyUniqueKeys = {};
        upsertStatement =
                dialect.getUpsertStatement(database, tableName, fieldNames, emptyUniqueKeys, false);

        assertFalse(upsertStatement.isPresent());
    }

    @Test
    public void testUpsertStatementWithQuotedIdentifiers() {
        String database = "WLS_TEST1";
        String tableName = "source_0924_sink2123"; // lowercase table name
        String[] fieldNames = {"tenant_id", "user_name", "content", "created_time"};
        String[] uniqueKeyFields = {"tenant_id", "user_name"};

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields, false);

        assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();

        System.out.println("Generated MERGE SQL with quoted identifiers:");
        System.out.println(sql);

        // Verify all identifiers are properly quoted
        assertTrue(sql.contains("MERGE INTO \"WLS_TEST1\".\"source_0924_sink2123\" AS target"));
        assertTrue(sql.contains("? AS \"tenant_id\""));
        assertTrue(sql.contains("? AS \"user_name\""));
        assertTrue(sql.contains("? AS \"content\""));
        assertTrue(sql.contains("? AS \"created_time\""));

        // Verify ON condition is properly quoted
        assertTrue(sql.contains("target.\"tenant_id\" = source.\"tenant_id\""));
        assertTrue(sql.contains("target.\"user_name\" = source.\"user_name\""));

        // Verify UPDATE clause is properly quoted
        assertTrue(sql.contains("\"content\" = source.\"content\""));
        assertTrue(sql.contains("\"created_time\" = source.\"created_time\""));

        // Verify INSERT clause is properly quoted
        assertTrue(
                sql.contains(
                        "INSERT (\"tenant_id\", \"user_name\", \"content\", \"created_time\")"));
        assertTrue(
                sql.contains(
                        "VALUES (source.\"tenant_id\", source.\"user_name\", source.\"content\", source.\"created_time\")"));

        // Verify unique key fields are not in UPDATE clause
        String updateClause = sql.substring(sql.indexOf("UPDATE SET"));
        assertFalse(updateClause.contains("\"tenant_id\" = source.\"tenant_id\""));
        assertFalse(updateClause.contains("\"user_name\" = source.\"user_name\""));
    }

    @Test
    public void testRowConverterAndTypeMapper() {
        assertNotNull(dialect.getRowConverter());
        assertTrue(dialect.getRowConverter() instanceof SnowflakeJdbcRowConverter);

        assertNotNull(dialect.getJdbcDialectTypeMapper());
        assertTrue(dialect.getJdbcDialectTypeMapper() instanceof SnowflakeTypeMapper);
    }

    @Test
    public void testQuoteIdentifier() {
        String result = dialect.quoteIdentifier("test_table");
        assertEquals("\"test_table\"", result);
    }

    @Test
    public void testQuoteDatabaseIdentifier() {
        String result = dialect.quoteDatabaseIdentifier("test_db");
        assertEquals("\"test_db\"", result);
    }

    @Test
    public void testTableIdentifier() {
        String result = dialect.tableIdentifier("WLS_TEST1.SCHEMA1", "source_0924_sink2123");
        assertEquals("\"WLS_TEST1.SCHEMA1\".\"source_0924_sink2123\"", result);

        System.out.println("Generated table identifier: " + result);
    }

    @Test
    public void testGetDeleteStatement() {
        String database = "WLS_TEST1.SCHEMA1";
        String tableName = "source_0924_sink2123";
        String[] conditionFields = {"id"};

        String deleteSQL = dialect.getDeleteStatement(database, tableName, conditionFields);

        System.out.println("Generated DELETE SQL:");
        System.out.println(deleteSQL);

        // The expected SQL should have properly quoted identifiers
        String expectedSQL =
                "DELETE FROM \"WLS_TEST1.SCHEMA1\".\"source_0924_sink2123\" WHERE \"id\" = :id";
        assertEquals(expectedSQL, deleteSQL);
    }

    @Test
    public void testGetDeleteStatementWithMultipleConditions() {
        String database = "WLS_TEST1.SCHEMA1";
        String tableName = "source_0924_sink2123";
        String[] conditionFields = {"id", "name"};

        String deleteSQL = dialect.getDeleteStatement(database, tableName, conditionFields);

        System.out.println("Generated DELETE SQL with multiple conditions:");
        System.out.println(deleteSQL);

        // The expected SQL should have properly quoted identifiers
        String expectedSQL =
                "DELETE FROM \"WLS_TEST1.SCHEMA1\".\"source_0924_sink2123\" WHERE \"id\" = :id AND \"name\" = :name";
        assertEquals(expectedSQL, deleteSQL);
    }

    @Test
    public void testGetDeleteStatementWithMixedCaseNames() {
        String database = "WLS_Test1.Schema1";
        String tableName = "Source_0924_Sink2123";
        String[] conditionFields = {"Id", "UserName"};

        String deleteSQL = dialect.getDeleteStatement(database, tableName, conditionFields);

        System.out.println("Generated DELETE SQL with mixed case:");
        System.out.println(deleteSQL);

        // The expected SQL should have properly quoted identifiers preserving case
        String expectedSQL =
                "DELETE FROM \"WLS_Test1.Schema1\".\"Source_0924_Sink2123\" WHERE \"Id\" = :Id AND \"UserName\" = :UserName";
        assertEquals(expectedSQL, deleteSQL);
    }
}
