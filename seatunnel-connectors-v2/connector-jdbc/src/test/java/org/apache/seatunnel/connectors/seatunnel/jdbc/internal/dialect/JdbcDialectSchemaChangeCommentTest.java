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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

public class JdbcDialectSchemaChangeCommentTest {

    @Test
    public void testApplySchemaChangeEscapesColumnComment() throws SQLException {
        JdbcDialect dialect = new TestJdbcDialect();
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        PhysicalColumn.builder()
                                .name("extra_info")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(32L)
                                .nullable(true)
                                .comment("owner's \\\\flag")
                                .sourceType("VARCHAR(32)")
                                .build());
        event.setSourceDialectName(dialect.dialectName());

        dialect.applySchemaChange(connection, TablePath.of("test_db", "test_table"), event);

        Mockito.verify(statement)
                .execute(
                        "ALTER TABLE test_db.test_table ADD COLUMN extra_info VARCHAR(32) NULL COMMENT 'owner''s \\\\\\\\flag'");
    }

    private static class TestJdbcDialect implements JdbcDialect {

        @Override
        public String dialectName() {
            return "test";
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypeConverter<BasicTypeDefine> typeConverter() {
            return (TypeConverter<BasicTypeDefine>)
                    (TypeConverter<?>) MySqlTypeConverter.DEFAULT_INSTANCE;
        }

        @Override
        public JdbcRowConverter getRowConverter() {
            return null;
        }

        @Override
        public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
            return null;
        }

        @Override
        public Optional<String> getUpsertStatement(
                String database,
                String tableName,
                String[] fieldNames,
                String[] uniqueKeyFields,
                boolean isPrimaryKeyUpdated) {
            return Optional.empty();
        }
    }
}
