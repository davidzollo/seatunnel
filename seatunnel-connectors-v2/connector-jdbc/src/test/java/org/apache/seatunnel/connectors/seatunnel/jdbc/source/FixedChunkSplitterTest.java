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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class FixedChunkSplitterTest {

    @Test
    void testQuerySplitUsesSplitJdbcUrl() throws SQLException {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(postgresConfig());
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("qa_source", "public", "orders"),
                        "split-query",
                        "SELECT * FROM \"public\".\"orders\" WHERE \"id\" > 0",
                        "jdbc:postgresql://localhost:5432/qa_source",
                        null,
                        null,
                        null,
                        null,
                        0,
                        1,
                        false);

        splitter.generateSplitStatement(split, TableSchema.builder().build());

        assertEquals("SELECT * FROM \"public\".\"orders\" WHERE \"id\" > 0", splitter.sql);
        assertEquals("jdbc:postgresql://localhost:5432/qa_source", splitter.jdbcUrl);
    }

    @Test
    void testHashSplitUsesSplitJdbcUrl() throws SQLException {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "tbl"),
                        "split-hash",
                        "SELECT * FROM `db`.`tbl` WHERE `id` = ?",
                        "jdbc:mysql://localhost:3306/shard_a",
                        "id",
                        BasicType.STRING_TYPE,
                        1,
                        null,
                        0,
                        4,
                        false);

        PreparedStatement statement =
                splitter.generateSplitStatement(split, TableSchema.builder().build());

        assertEquals("SELECT * FROM `db`.`tbl` WHERE `id` = ?", splitter.sql);
        assertEquals("jdbc:mysql://localhost:3306/shard_a", splitter.jdbcUrl);
        verify(statement).setInt(1, 1);
    }

    private static JdbcSourceConfig mysqlConfig() {
        return JdbcSourceConfig.builder()
                .jdbcConnectionConfig(
                        JdbcConnectionConfig.builder()
                                .url("jdbc:mysql://localhost:3306/test")
                                .driverName("com.mysql.cj.jdbc.Driver")
                                .build())
                .build();
    }

    private static JdbcSourceConfig postgresConfig() {
        return JdbcSourceConfig.builder()
                .jdbcConnectionConfig(
                        JdbcConnectionConfig.builder()
                                .url("jdbc:postgresql://localhost:5432/postgres")
                                .driverName("org.postgresql.Driver")
                                .build())
                .build();
    }

    private static class CapturingFixedChunkSplitter extends FixedChunkSplitter {
        private String sql;
        private String jdbcUrl;

        private CapturingFixedChunkSplitter(JdbcSourceConfig config) {
            super(config);
        }

        @Override
        protected PreparedStatement createPreparedStatement(String sql, String jdbcUrl) {
            this.sql = sql;
            this.jdbcUrl = jdbcUrl;
            return mock(PreparedStatement.class);
        }
    }
}
