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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.phoenix;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class PhoenixCatalog extends AbstractJdbcCatalog {

    private ReadonlyConfig options;

    public PhoenixCatalog(String catalogName, JdbcUrlUtil.UrlInfo urlInfo, ReadonlyConfig options) {
        super(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
        this.options = options;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList("default");
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tableNames = new ArrayList<>();
        String querySql = "select * from system.catalog where table_type = 'u'";
        try (Connection connection = getConnection(baseUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(querySql)) {

            while (resultSet.next()) {
                String tableSchema = resultSet.getString("TABLE_SCHEM");
                String tableName = resultSet.getString("TABLE_NAME");
                if (StringUtils.isNotEmpty(tableSchema)) {
                    tableNames.add(tableSchema + "." + tableName);
                } else {
                    tableNames.add(tableName);
                }
            }
            return tableNames;
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    protected Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            this.doKerberosAuthentication();
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format("!describe \"%s\"", tablePath.getTableName());
    }

    @Override
    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        PhoenixCreateTableSqlBuilder phoenixCreateTableSqlBuilder =
                new PhoenixCreateTableSqlBuilder(table);
        try {
            String createTableSql = phoenixCreateTableSqlBuilder.build(tablePath);
            executeInternal(baseUrl, createTableSql);
            if (phoenixCreateTableSqlBuilder.isHaveConstraintKey) {
                String alterTableSql =
                        "ALTER TABLE "
                                + tablePath.getSchemaAndTableName("\"")
                                + " REPLICA IDENTITY FULL;";
                executeInternal(baseUrl, alterTableSql);
            }
            if (CollectionUtils.isNotEmpty(phoenixCreateTableSqlBuilder.getCreateIndexSqls())) {
                for (String createIndexSql : phoenixCreateTableSqlBuilder.getCreateIndexSqls()) {
                    executeInternal(baseUrl, createIndexSql);
                }
            }

        } catch (Exception ex) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), ex);
        }
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new PhoenixCreateTableSqlBuilder(table).build(tablePath);
    }

    @Override
    protected boolean executeInternal(String url, String sql) throws SQLException {
        log.info("Execute sql : {}", sql.replace("\n", " "));
        try (Statement ps = getConnection(url).createStatement()) {
            return ps.execute(sql);
        }
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        return StringUtils.isNotEmpty(schemaName)
                ? "DROP TABLE \""
                        + tablePath.getSchemaName()
                        + "\".\""
                        + tablePath.getTableName()
                        + "\""
                : "DROP TABLE \"" + tablePath.getTableName() + "\"";
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return "CREATE DATABASE \"" + databaseName + "\"";
    }

    public String getExistDataSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return StringUtils.isNotEmpty(schemaName)
                ? String.format("select * from \"%s\".\"%s\" limit 1", schemaName, tableName)
                : String.format("select * from \"%s\" limit 1", tableName);
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return StringUtils.isNotEmpty(schemaName)
                ? "delete from \"" + schemaName + "\".\"" + tableName + "\""
                : "delete from \"" + tableName + "\"";
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return "DROP DATABASE \"" + databaseName + "\"";
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "select * from system.catalog where table_type = 'u' and TABLE_SCHEM = '%s' AND TABLE_NAME = '%s'",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @SneakyThrows
    private void doKerberosAuthentication() {
        String principal = options.get(JdbcOptions.KERBEROS_PRINCIPAL);
        if (StringUtils.isEmpty(principal)) {
            return;
        }
        String kerberosKrb5ConfPath = options.get(JdbcOptions.KRB5_PATH);
        String keytabPath = options.get(JdbcOptions.KERBEROS_KEYTAB_PATH);
        System.setProperty("java.security.krb5.conf", kerberosKrb5ConfPath);
        Configuration configuration = new Configuration();
        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
            log.warn(
                    "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
                    principal,
                    keytabPath);
        } else {
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                log.info(
                        "Start Kerberos authentication using principal {} and keytab {}",
                        principal,
                        keytabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                log.info("Kerberos authentication successful");
            } catch (IOException e) {
                throw new SeaTunnelException(
                        "check hive connectivity failed, " + e.getMessage(), e);
            }
        }
    }
}
