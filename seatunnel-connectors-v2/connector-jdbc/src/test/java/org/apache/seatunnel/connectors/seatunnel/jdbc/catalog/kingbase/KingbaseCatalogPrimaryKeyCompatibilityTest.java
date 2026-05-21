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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class KingbaseCatalogPrimaryKeyCompatibilityTest {

    @Test
    void shouldResolvePrimaryKeyFromPgCatalogQuery() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement probeStatement = Mockito.mock(Statement.class);
        ResultSet probeResultSet = Mockito.mock(ResultSet.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);

        Mockito.when(connection.createStatement()).thenReturn(probeStatement);
        Mockito.when(probeStatement.executeQuery(Mockito.anyString())).thenReturn(probeResultSet);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getString("pk_name"))
                .thenReturn("wt_kb_pgoutput_verify_pkey", "wt_kb_pgoutput_verify_pkey");
        Mockito.when(resultSet.getString("column_name")).thenReturn("id", "tenant_id");
        Mockito.when(resultSet.getInt("key_seq")).thenReturn(1, 2);

        TestKingbaseCatalog catalog =
                new TestKingbaseCatalog(
                        JdbcUrlUtil.getUrlInfo("jdbc:kingbase8://127.0.0.1:54321/test"),
                        connection);
        catalog.open();

        Optional<PrimaryKey> primaryKey =
                catalog.resolvePrimaryKey(
                        metaData, TablePath.of("test", "public", "wt_kb_pgoutput_verify"));

        Assertions.assertTrue(primaryKey.isPresent());
        Assertions.assertEquals("wt_kb_pgoutput_verify_pkey", primaryKey.get().getPrimaryKey());
        Assertions.assertEquals(
                Arrays.asList("id", "tenant_id"), primaryKey.get().getColumnNames());
        Mockito.verify(connection).prepareStatement(Mockito.contains("con.conkey::int[]"));
        Mockito.verify(statement).setString(1, "public");
        Mockito.verify(statement).setString(2, "wt_kb_pgoutput_verify");
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    void shouldResolveConstraintKeysFromPgCatalogQuery() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement probeStatement = Mockito.mock(Statement.class);
        ResultSet probeResultSet = Mockito.mock(ResultSet.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);

        Mockito.when(connection.createStatement()).thenReturn(probeStatement);
        Mockito.when(probeStatement.executeQuery(Mockito.anyString())).thenReturn(probeResultSet);
        Mockito.when(connection.prepareStatement(Mockito.contains("FROM pg_index")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, true, true, false);
        Mockito.when(resultSet.getString("index_name"))
                .thenReturn(
                        "wt_kb_pgoutput_verify_note_idx",
                        "wt_kb_pgoutput_verify_note_idx",
                        "wt_kb_pgoutput_verify_tenant_uk");
        Mockito.when(resultSet.getBoolean("is_unique")).thenReturn(false, false, true);
        Mockito.when(resultSet.getString("column_name")).thenReturn("note", "created_at", "id");

        TestKingbaseCatalog catalog =
                new TestKingbaseCatalog(
                        JdbcUrlUtil.getUrlInfo("jdbc:kingbase8://127.0.0.1:54321/test"),
                        connection);
        catalog.open();

        List<ConstraintKey> constraintKeys =
                catalog.resolveConstraintKeys(
                        metaData, TablePath.of("test", "public", "wt_kb_pgoutput_verify"));

        Assertions.assertEquals(2, constraintKeys.size());
        Assertions.assertEquals(
                ConstraintKey.ConstraintType.INDEX_KEY, constraintKeys.get(0).getConstraintType());
        Assertions.assertEquals(
                Arrays.asList("note", "created_at"),
                Arrays.asList(
                        constraintKeys.get(0).getColumnNames().get(0).getColumnName(),
                        constraintKeys.get(0).getColumnNames().get(1).getColumnName()));
        Assertions.assertEquals(
                ConstraintKey.ConstraintType.UNIQUE_KEY, constraintKeys.get(1).getConstraintType());
        Assertions.assertEquals(
                "id", constraintKeys.get(1).getColumnNames().get(0).getColumnName());
        Mockito.verify(connection).prepareStatement(Mockito.contains("ind.indkey::int[]"));
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    void shouldFallbackToLegacyCatalogAndMetadataWhenPgProbeFails() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement probeStatement = Mockito.mock(Statement.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        ResultSet primaryKeyResultSet = Mockito.mock(ResultSet.class);
        ResultSet constraintKeyResultSet = Mockito.mock(ResultSet.class);

        Mockito.when(connection.createStatement()).thenReturn(probeStatement);
        Mockito.when(probeStatement.executeQuery(Mockito.anyString()))
                .thenThrow(new SQLException("pg catalogs unavailable"));

        Mockito.when(
                        metaData.getPrimaryKeys(
                                Mockito.eq("test"),
                                Mockito.eq("public"),
                                Mockito.eq("wt_kb_pgoutput_verify")))
                .thenReturn(primaryKeyResultSet);
        Mockito.when(primaryKeyResultSet.next()).thenReturn(true, false);
        Mockito.when(primaryKeyResultSet.getString("COLUMN_NAME")).thenReturn("id");
        Mockito.when(primaryKeyResultSet.getString("PK_NAME"))
                .thenReturn("wt_kb_pgoutput_verify_pkey");
        Mockito.when(primaryKeyResultSet.getInt("KEY_SEQ")).thenReturn(1);

        Mockito.when(
                        metaData.getIndexInfo(
                                Mockito.eq("test"),
                                Mockito.eq("public"),
                                Mockito.eq("wt_kb_pgoutput_verify"),
                                Mockito.eq(false),
                                Mockito.eq(true)))
                .thenReturn(constraintKeyResultSet);
        Mockito.when(constraintKeyResultSet.next()).thenReturn(true, false);
        Mockito.when(constraintKeyResultSet.getString("COLUMN_NAME")).thenReturn("note");
        Mockito.when(constraintKeyResultSet.getString("INDEX_NAME"))
                .thenReturn("wt_kb_pgoutput_verify_note_idx");
        Mockito.when(constraintKeyResultSet.getBoolean("NON_UNIQUE")).thenReturn(true);
        Mockito.when(constraintKeyResultSet.getString("ASC_OR_DESC")).thenReturn("A");

        TestKingbaseCatalog catalog =
                new TestKingbaseCatalog(
                        JdbcUrlUtil.getUrlInfo("jdbc:kingbase8://127.0.0.1:54321/test"),
                        connection);
        catalog.open();

        Assertions.assertTrue(
                catalog.getSelectColumnsSqlForTest(
                                TablePath.of("test", "public", "wt_kb_pgoutput_verify"))
                        .contains("sys_class"));
        Assertions.assertTrue(
                catalog.getDatabaseWithConditionSqlForTest("test").contains("sys_database"));

        Optional<PrimaryKey> primaryKey =
                catalog.resolvePrimaryKey(
                        metaData, TablePath.of("test", "public", "wt_kb_pgoutput_verify"));
        List<ConstraintKey> constraintKeys =
                catalog.resolveConstraintKeys(
                        metaData, TablePath.of("test", "public", "wt_kb_pgoutput_verify"));

        Assertions.assertTrue(primaryKey.isPresent());
        Assertions.assertEquals(Arrays.asList("id"), primaryKey.get().getColumnNames());
        Assertions.assertEquals(1, constraintKeys.size());
        Assertions.assertEquals(
                "note", constraintKeys.get(0).getColumnNames().get(0).getColumnName());
        Mockito.verify(connection, Mockito.never()).prepareStatement(Mockito.anyString());
    }

    @Test
    void shouldResolveTableCommentFromPgCatalogQuery() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement probeStatement = Mockito.mock(Statement.class);
        ResultSet probeResultSet = Mockito.mock(ResultSet.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);

        Mockito.when(connection.createStatement()).thenReturn(probeStatement);
        Mockito.when(probeStatement.executeQuery(Mockito.anyString())).thenReturn(probeResultSet);
        Mockito.when(connection.prepareStatement(Mockito.contains("obj_description")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(1)).thenReturn("kingbase table comment");

        TestKingbaseCatalog catalog =
                new TestKingbaseCatalog(
                        JdbcUrlUtil.getUrlInfo("jdbc:kingbase8://127.0.0.1:54321/test"),
                        connection);
        catalog.open();

        String comment =
                catalog.resolveTableComment(metaData, TablePath.of("test", "public", "demo"));

        Assertions.assertEquals("kingbase table comment", comment);
        Mockito.verify(connection).prepareStatement(Mockito.contains("obj_description"));
        Mockito.verifyNoInteractions(metaData);
    }

    private static final class TestKingbaseCatalog extends KingbaseCatalog {

        private final Connection connection;

        private TestKingbaseCatalog(JdbcUrlUtil.UrlInfo urlInfo, Connection connection) {
            super("kingbase", "system", "secret", urlInfo, "public");
            this.connection = connection;
        }

        private Optional<PrimaryKey> resolvePrimaryKey(
                DatabaseMetaData metaData, TablePath tablePath) throws SQLException {
            return super.getPrimaryKey(metaData, tablePath);
        }

        private List<ConstraintKey> resolveConstraintKeys(
                DatabaseMetaData metaData, TablePath tablePath) throws SQLException {
            return super.getConstraintKeys(metaData, tablePath);
        }

        private String getSelectColumnsSqlForTest(TablePath tablePath) {
            return super.getSelectColumnsSql(tablePath);
        }

        private String getDatabaseWithConditionSqlForTest(String databaseName) {
            return super.getDatabaseWithConditionSql(databaseName);
        }

        private String resolveTableComment(DatabaseMetaData metaData, TablePath tablePath)
                throws SQLException {
            return super.getTableComment(metaData, tablePath);
        }

        @Override
        protected Connection getConnection(String url) {
            return connection;
        }
    }
}
