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
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MySqlDdlParserTest {

    private static final String CATALOG_NAME = "test_catalog";
    private static final String DATABASE_NAME = "test_db";

    private MySqlAntlrDdlParser parser;

    @BeforeEach
    public void setUp() {
        parser = new MySqlAntlrDdlParser(";", false, CATALOG_NAME, null);
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
}
