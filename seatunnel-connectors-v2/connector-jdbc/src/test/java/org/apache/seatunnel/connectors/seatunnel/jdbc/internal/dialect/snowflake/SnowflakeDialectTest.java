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
        assertTrue(sql.contains("MERGE INTO test_db.test_table AS target"));
        assertTrue(
                sql.contains(
                        "USING (SELECT ? AS id, ? AS name, ? AS value, ? AS updated_time) AS source"));
        assertTrue(sql.contains("ON target.id = source.id"));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assertTrue(sql.contains("name = source.name"));
        assertTrue(sql.contains("value = source.value"));
        assertTrue(sql.contains("updated_time = source.updated_time"));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT"));
        assertTrue(
                sql.contains("VALUES (source.id, source.name, source.value, source.updated_time)"));

        // Verify id field is not in UPDATE clause (as it's the unique key)
        String updateClause = sql.substring(sql.indexOf("UPDATE SET"));
        assertFalse(updateClause.contains("id = source.id"));
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
                        "ON target.tenant_id = source.tenant_id AND target.user_id = source.user_id"));

        // Verify only non-key fields are updated
        assertTrue(sql.contains("name = source.name"));
        assertTrue(sql.contains("value = source.value"));
        String updateClause = sql.substring(sql.indexOf("UPDATE SET"));
        assertFalse(updateClause.contains("tenant_id = source.tenant_id"));
        assertFalse(updateClause.contains("user_id = source.user_id"));
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
        assertTrue(sql.contains("MERGE INTO test_table AS target"));
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
    public void testRowConverterAndTypeMapper() {
        assertNotNull(dialect.getRowConverter());
        assertTrue(dialect.getRowConverter() instanceof SnowflakeJdbcRowConverter);

        assertNotNull(dialect.getJdbcDialectTypeMapper());
        assertTrue(dialect.getJdbcDialectTypeMapper() instanceof SnowflakeTypeMapper);
    }
}
