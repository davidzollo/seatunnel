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

package io.debezium.connector.kingbase.connection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

public class KingbaseConnectionTest {

    @Test
    public void testDefaultSettingsPreserveExplicitJdbcFlags() {
        KingbaseConnection connection =
                new KingbaseConnection(
                        Configuration.create()
                                .with("hostname", "127.0.0.1")
                                .with("port", "54321")
                                .with("user", "system")
                                .with("password", "kingbase")
                                .with("dbname", "test")
                                .with("sslmode", "disable")
                                .with("ApplicationName", "wt-ui")
                                .build());

        Assertions.assertEquals("9.4", connection.config().getString("assumeMinServerVersion"));
        Assertions.assertEquals("disable", connection.config().getString("sslmode"));
        Assertions.assertEquals("wt-ui", connection.config().getString("ApplicationName"));
    }

    @Test
    public void testRawJdbcUrlOverridesPatternConnectionString() {
        KingbaseConnection connection =
                new KingbaseConnection(
                        Configuration.create()
                                .with(
                                        "url",
                                        "jdbc:kingbase8://127.0.0.1:65422/test?sslmode=disable")
                                .with("hostname", "192.168.1.10")
                                .with("port", "54321")
                                .with("user", "system")
                                .with("password", "kingbase")
                                .with("dbname", "test")
                                .build());

        Assertions.assertEquals(
                "jdbc:kingbase8://127.0.0.1:65422/test?sslmode=disable",
                connection.connectionString());
        Assertions.assertEquals("9.4", connection.config().getString("assumeMinServerVersion"));
    }

    @Test
    public void testReadPrimaryKeyNamesUsesPgCatalogQuery() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM pg_constraint")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getString("column_name")).thenReturn("id", "tenant_id");
        Mockito.when(resultSet.getInt("key_seq")).thenReturn(1, 2);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, true);
        List<String> primaryKeys =
                connection.readPrimaryKeyNames(
                        metaData, new TableId(null, "public", "wt_kb_pgoutput_verify"));

        Assertions.assertEquals(Arrays.asList("id", "tenant_id"), primaryKeys);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM pg_constraint"));
        Mockito.verify(statement).setString(1, "public");
        Mockito.verify(statement).setString(2, "wt_kb_pgoutput_verify");
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    public void testReadTableUniqueIndicesUsesPgCatalogQuery() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM pg_index i")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, true, true, false);
        Mockito.when(resultSet.getString("index_name"))
                .thenReturn("wt_idx_unique", "wt_idx_unique", "wt_idx_unique_2");
        Mockito.when(resultSet.getString("column_name"))
                .thenReturn("tenant_id", "order_id", "ignored_col");
        Mockito.when(resultSet.getInt("column_index")).thenReturn(1, 2, 1);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, true);
        List<String> uniqueIndexColumns =
                connection.readTableUniqueIndices(
                        metaData, new TableId(null, "public", "wt_kb_pgoutput_verify"));

        Assertions.assertEquals(Arrays.asList("tenant_id", "order_id"), uniqueIndexColumns);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM pg_index i"));
        Mockito.verify(statement).setString(1, "public");
        Mockito.verify(statement).setString(2, "wt_kb_pgoutput_verify");
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    public void testReadPrimaryKeyNamesFallsBackToSysCatalogQuery() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM sys_constraint")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getString("column_name")).thenReturn("id");
        Mockito.when(resultSet.getInt("key_seq")).thenReturn(1);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, false);
        List<String> primaryKeys =
                connection.readPrimaryKeyNames(metaData, new TableId(null, "public", "wt_kb_sys"));

        Assertions.assertEquals(Arrays.asList("id"), primaryKeys);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM sys_constraint"));
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    public void testReadPrimaryKeyNamesRetriesSysCatalogAfterPgFailure() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement pgStatement = Mockito.mock(PreparedStatement.class);
        PreparedStatement sysStatement = Mockito.mock(PreparedStatement.class);
        ResultSet sysResultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM pg_constraint")))
                .thenReturn(pgStatement);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM sys_constraint")))
                .thenReturn(sysStatement);
        Mockito.when(pgStatement.executeQuery()).thenThrow(new java.sql.SQLException("pg missing"));
        Mockito.when(sysStatement.executeQuery()).thenReturn(sysResultSet);
        Mockito.when(sysResultSet.next()).thenReturn(true, false);
        Mockito.when(sysResultSet.getString("column_name")).thenReturn("id");
        Mockito.when(sysResultSet.getInt("key_seq")).thenReturn(1);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, true);
        List<String> primaryKeys =
                connection.readPrimaryKeyNames(metaData, new TableId(null, "public", "wt_kb_sys"));

        Assertions.assertEquals(Arrays.asList("id"), primaryKeys);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM pg_constraint"));
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM sys_constraint"));
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    public void testReadTableUniqueIndicesFallsBackToSysCatalogQuery() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM sys_index i")))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getString("index_name")).thenReturn("wt_idx_sys_unique");
        Mockito.when(resultSet.getString("column_name")).thenReturn("tenant_id");
        Mockito.when(resultSet.getInt("column_index")).thenReturn(1);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, false);
        List<String> uniqueIndexColumns =
                connection.readTableUniqueIndices(
                        metaData, new TableId(null, "public", "wt_kb_sys"));

        Assertions.assertEquals(Arrays.asList("tenant_id"), uniqueIndexColumns);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM sys_index i"));
        Mockito.verifyNoInteractions(metaData);
    }

    @Test
    public void testReadTableUniqueIndicesRetriesSysCatalogAfterPgFailure() throws Exception {
        Connection jdbc = Mockito.mock(Connection.class);
        PreparedStatement pgStatement = Mockito.mock(PreparedStatement.class);
        PreparedStatement sysStatement = Mockito.mock(PreparedStatement.class);
        ResultSet sysResultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM pg_index i")))
                .thenReturn(pgStatement);
        Mockito.when(jdbc.prepareStatement(Mockito.contains("FROM sys_index i")))
                .thenReturn(sysStatement);
        Mockito.when(pgStatement.executeQuery()).thenThrow(new java.sql.SQLException("pg missing"));
        Mockito.when(sysStatement.executeQuery()).thenReturn(sysResultSet);
        Mockito.when(sysResultSet.next()).thenReturn(true, false);
        Mockito.when(sysResultSet.getString("index_name")).thenReturn("wt_idx_sys_unique");
        Mockito.when(sysResultSet.getString("column_name")).thenReturn("tenant_id");
        Mockito.when(sysResultSet.getInt("column_index")).thenReturn(1);

        KingbaseConnection connection = new TestKingbaseConnection(jdbc, true);
        List<String> uniqueIndexColumns =
                connection.readTableUniqueIndices(
                        metaData, new TableId(null, "public", "wt_kb_sys"));

        Assertions.assertEquals(Arrays.asList("tenant_id"), uniqueIndexColumns);
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM pg_index i"));
        Mockito.verify(jdbc).prepareStatement(Mockito.contains("FROM sys_index i"));
        Mockito.verifyNoInteractions(metaData);
    }

    private static final class TestKingbaseConnection extends KingbaseConnection {

        private final Connection connection;
        private final boolean usePostgresCatalog;

        private TestKingbaseConnection(Connection connection, boolean usePostgresCatalog) {
            super(
                    Configuration.create()
                            .with("hostname", "127.0.0.1")
                            .with("port", "54321")
                            .with("user", "system")
                            .with("password", "kingbase")
                            .with("dbname", "test")
                            .build());
            this.connection = connection;
            this.usePostgresCatalog = usePostgresCatalog;
        }

        @Override
        public synchronized Connection connection() {
            return connection;
        }

        @Override
        protected boolean shouldUsePostgresCatalog() {
            return usePostgresCatalog;
        }
    }
}
