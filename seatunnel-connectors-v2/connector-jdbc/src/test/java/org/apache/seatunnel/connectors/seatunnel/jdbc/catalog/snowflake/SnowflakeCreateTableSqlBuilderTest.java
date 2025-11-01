/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnowflakeCreateTableSqlBuilderTest {

    @Test
    public void testBuildCreateTableSqlWithComments() {
        // Create columns with comments
        Column idColumn =
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.LONG_TYPE)
                        .nullable(false)
                        .comment("主键ID")
                        .build();

        Column nameColumn =
                PhysicalColumn.builder()
                        .name("name")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(false)
                        .comment("用户名")
                        .build();

        Column contentColumn =
                PhysicalColumn.builder()
                        .name("content")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .comment("内容")
                        .build();

        // Create primary key
        PrimaryKey primaryKey = PrimaryKey.of("pk_id", Collections.singletonList("id"));

        // Create table schema
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(Arrays.asList(idColumn, nameColumn, contentColumn))
                        .primaryKey(primaryKey)
                        .build();

        // Create catalog table
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_database", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Test table with comments");

        // Build SQL
        SnowflakeCreateTableSqlBuilder builder = new SnowflakeCreateTableSqlBuilder(catalogTable);
        String sql = builder.build("\"test_database\".\"test_table\"");

        System.out.println("Generated SQL with comments:");
        System.out.println(sql);

        // Verify SQL contains inline comments
        assertTrue(
                sql.contains("COMMENT '主键ID'"), "SQL should contain inline comment for id column");
        assertTrue(
                sql.contains("COMMENT '用户名'"), "SQL should contain inline comment for name column");
        assertTrue(
                sql.contains("COMMENT '内容'"),
                "SQL should contain inline comment for content column");

        // Verify SQL structure
        assertTrue(
                sql.startsWith("CREATE TABLE IF NOT EXISTS"),
                "SQL should start with CREATE TABLE IF NOT EXISTS");
        assertTrue(
                sql.contains("\"id\" BIGINT NOT NULL PRIMARY KEY COMMENT '主键ID'"),
                "ID column should have proper structure");
        assertTrue(
                sql.contains("\"name\" STRING NOT NULL COMMENT '用户名'"),
                "Name column should have proper structure");
        assertTrue(
                sql.contains("\"content\" STRING COMMENT '内容'"),
                "Content column should have proper structure");
    }

    @Test
    public void testBuildCreateTableSqlWithoutComments() {
        // Create columns without comments
        Column idColumn =
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.LONG_TYPE)
                        .nullable(false)
                        .build();

        Column nameColumn =
                PhysicalColumn.builder()
                        .name("name")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(false)
                        .build();

        // Create primary key
        PrimaryKey primaryKey = PrimaryKey.of("pk_id", Collections.singletonList("id"));

        // Create table schema
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(Arrays.asList(idColumn, nameColumn))
                        .primaryKey(primaryKey)
                        .build();

        // Create catalog table
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_database", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Test table without comments");

        // Build SQL
        SnowflakeCreateTableSqlBuilder builder = new SnowflakeCreateTableSqlBuilder(catalogTable);
        String sql = builder.build("\"test_database\".\"test_table\"");

        System.out.println("Generated SQL without comments:");
        System.out.println(sql);

        // Verify SQL does not contain COMMENT keywords
        assertTrue(!sql.contains("COMMENT"), "SQL should not contain any COMMENT keywords");

        // Verify SQL structure
        assertTrue(
                sql.startsWith("CREATE TABLE IF NOT EXISTS"),
                "SQL should start with CREATE TABLE IF NOT EXISTS");
        assertTrue(
                sql.contains("\"id\" BIGINT NOT NULL PRIMARY KEY"),
                "ID column should have proper structure");
        assertTrue(
                sql.contains("\"name\" STRING NOT NULL"),
                "Name column should have proper structure");
    }

    @Test
    public void testBuildColumnSqlWithSpecialCharacters() {
        // Create column with special characters in comment
        Column column =
                PhysicalColumn.builder()
                        .name("description")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .comment("这是一个包含'单引号'的注释")
                        .build();

        // Create simple table schema
        TableSchema tableSchema =
                TableSchema.builder().columns(Collections.singletonList(column)).build();

        // Create catalog table
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_database", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Test table");

        // Build SQL
        SnowflakeCreateTableSqlBuilder builder = new SnowflakeCreateTableSqlBuilder(catalogTable);
        String sql = builder.build("\"test_database\".\"test_table\"");

        System.out.println("Generated SQL with special characters:");
        System.out.println(sql);

        // Verify that single quotes in comments are properly escaped
        assertTrue(
                sql.contains("COMMENT '这是一个包含''单引号''的注释'"),
                "Single quotes in comments should be escaped");
    }
}
