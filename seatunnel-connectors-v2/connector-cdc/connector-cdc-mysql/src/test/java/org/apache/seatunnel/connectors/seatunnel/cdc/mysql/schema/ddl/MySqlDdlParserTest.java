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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

import java.sql.Types;
import java.util.List;

public class MySqlDdlParserTest {

    private static final String CATALOG_NAME = "test_catalog";
    private static final String DATABASE_NAME = "test_db";

    private MySqlAntlrDdlParser parser;

    @BeforeEach
    public void setUp() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        MySqlSourceConfig sourceConfig = factory.create(0);

        parser = new MySqlAntlrDdlParser(";", false, CATALOG_NAME, null, sourceConfig);
        parser.setCurrentDatabase(DATABASE_NAME);
    }

    @Test
    public void testParseAddColumnComment() {
        parser.parse(
                "ALTER TABLE test_table ADD COLUMN extra_info VARCHAR(32) COMMENT 'added comment';",
                null);

        List<SchemaChangeEvent> schemaChangeEvents = parser.getSchemaChanges().getEvents();
        Assertions.assertEquals(1, schemaChangeEvents.size());

        AlterTableColumnsEvent alterTableColumnsEvent =
                (AlterTableColumnsEvent) schemaChangeEvents.get(0);
        Assertions.assertEquals(1, alterTableColumnsEvent.getEvents().size());

        AlterTableAddColumnEvent addColumnEvent =
                (AlterTableAddColumnEvent) alterTableColumnsEvent.getEvents().get(0);
        Column column = addColumnEvent.getColumn();

        Assertions.assertEquals("extra_info", column.getName());
        Assertions.assertEquals("added comment", column.getComment());
        Assertions.assertEquals("VARCHAR(32)", column.getSourceType());
    }

    @Test
    public void testParseAddColumnCommentWithEscapedLiteral() {
        parser.parse(
                "ALTER TABLE test_table ADD COLUMN extra_info VARCHAR(32) COMMENT 'owner''s \\\\flag';",
                null);

        List<SchemaChangeEvent> schemaChangeEvents = parser.getSchemaChanges().getEvents();
        AlterTableColumnsEvent alterTableColumnsEvent =
                (AlterTableColumnsEvent) schemaChangeEvents.get(0);
        AlterTableAddColumnEvent addColumnEvent =
                (AlterTableAddColumnEvent) alterTableColumnsEvent.getEvents().get(0);

        Assertions.assertEquals("owner's \\flag", addColumnEvent.getColumn().getComment());
    }

    @Test
    public void testParseModifyColumnWithoutCommentDropsExistingComment() {
        parser.parse(
                "ALTER TABLE test_table MODIFY COLUMN extra_info VARCHAR(64);",
                createTableEditor(
                        "test_table", createVarcharColumn("extra_info", 32, "legacy comment")));

        List<SchemaChangeEvent> schemaChangeEvents = parser.getSchemaChanges().getEvents();
        AlterTableColumnsEvent alterTableColumnsEvent =
                (AlterTableColumnsEvent) schemaChangeEvents.get(schemaChangeEvents.size() - 1);
        AlterTableModifyColumnEvent modifyColumnEvent =
                (AlterTableModifyColumnEvent) alterTableColumnsEvent.getEvents().get(0);

        Assertions.assertNull(modifyColumnEvent.getColumn().getComment());
    }

    @Test
    public void testParseChangeColumnRenameWithComment() {
        parser.parse(
                "ALTER TABLE test_table CHANGE COLUMN old_c new_c VARCHAR(64) COMMENT 'new comment';",
                createTableEditor("test_table", createVarcharColumn("old_c", 32, null)));

        List<SchemaChangeEvent> schemaChangeEvents = parser.getSchemaChanges().getEvents();
        AlterTableColumnsEvent alterTableColumnsEvent =
                (AlterTableColumnsEvent) schemaChangeEvents.get(schemaChangeEvents.size() - 1);
        AlterTableChangeColumnEvent changeColumnEvent =
                (AlterTableChangeColumnEvent) alterTableColumnsEvent.getEvents().get(0);

        Assertions.assertEquals("old_c", changeColumnEvent.getOldColumn());
        Assertions.assertEquals("new_c", changeColumnEvent.getColumn().getName());
        Assertions.assertEquals("new comment", changeColumnEvent.getColumn().getComment());
        Assertions.assertEquals("VARCHAR(64)", changeColumnEvent.getColumn().getSourceType());
    }

    // Alter-column parsing depends on the current table metadata, so tests build a minimal editor.
    private TableEditor createTableEditor(
            String tableName, io.debezium.relational.Column... columns) {
        return Table.editor()
                .tableId(new TableId(CATALOG_NAME, DATABASE_NAME, tableName))
                .addColumns(columns);
    }

    private io.debezium.relational.Column createVarcharColumn(
            String columnName, int length, String comment) {
        return io.debezium.relational.Column.editor()
                .name(columnName)
                .jdbcType(Types.VARCHAR)
                .type("VARCHAR", "VARCHAR(" + length + ")")
                .length(length)
                .optional(true)
                .comment(comment)
                .create();
    }
}
