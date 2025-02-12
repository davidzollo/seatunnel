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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.inceptor;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorJdbcUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class InceptorCatalog extends AbstractJdbcCatalog {
    private static final String QUOTE = "`";

    private ReadonlyConfig options;

    public InceptorCatalog(
            String catalogName, JdbcUrlUtil.UrlInfo urlInfo, ReadonlyConfig options) {
        super(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
        this.options = options;
    }

    @Override
    protected String getListDatabaseSql() {
        return "select database_name from system.databases_v";
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(
                "select database_name from system.databases_v where database_name = '%s'",
                databaseName);
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "select table_name from system.tables_v where database_name = '%s'", databaseName);
    }

    @Override
    protected String getListViewSql(String databaseName) {
        return String.format(
                "select table_name from system.views_v where database_name = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "select table_name from system.tables_v where database_name = '%s' and table_name = '%s'",
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        // Inceptor does not support constraint key
        return Collections.emptyList();
    }

    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        // Inceptor does not support primary key
        return Optional.empty();
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getFullName();
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        StringBuilder query = new StringBuilder();
        query.append("select * from system.columns_v");
        query.append(" where");
        if (tablePath.getDatabaseName() != null) {
            query.append(" database_name = '").append(tablePath.getDatabaseName()).append("'");
            query.append(" and");
        }
        query.append(" table_name = '").append(tablePath.getTableName()).append("'");
        query.append(" order by column_id asc");
        return query.toString();
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        // e.g. decimal(10, 2)
        String columnType = resultSet.getString("column_type");
        // e.g. decimal
        String dataType =
                columnType.contains("(")
                        ? columnType.substring(0, columnType.indexOf("("))
                        : columnType;
        String comment = resultSet.getString("commentstring");
        Object defaultValue = resultSet.getObject("default_value");
        boolean nullable = resultSet.getBoolean("nullable");
        boolean uniqueConstraint = resultSet.getBoolean("unique_constraint");
        // e.g. `decimal(10, 2)` is 10
        long columnLength = resultSet.getInt("column_length");
        // e.g. `decimal(10, 2)` is 2
        int numberScale = resultSet.getInt("column_scale");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(numberScale)
                        .nullable(nullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return InceptorTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("SELECT * FROM %s LIMIT 1;", tablePath.getFullNameWithQuoted("`"));
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return "CREATE DATABASE IF NOT EXISTS " + databaseName;
    }

    protected String getTruncateTableSql(TablePath tablePath) {
        return "TRUNCATE TABLE " + tablePath.getFullNameWithQuoted(QUOTE);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return "DROP TABLE " + tablePath.getFullNameWithQuoted(QUOTE);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return "DROP DATABASE " + QUOTE + databaseName + QUOTE;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        String sql = "SELECT * FROM (" + sqlQuery + ") t LIMIT 1";
        Connection defaultConnection = getConnection(defaultUrl);
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery(); ) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            return CatalogUtils.getCatalogTable(
                    resultSetMetaData, new InceptorTypeMapper(), sqlQuery);
        }
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new InceptorCreateTableSqlBuilder(tablePath.getTableName(), table).build();
    }

    protected Connection getConnection(TablePath tablePath) {
        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        return getConnection(dbUrl);
    }

    @Override
    protected Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            if (options.getOptional(JdbcOptions.KERBEROS_PRINCIPAL).isPresent()) {
                InceptorJdbcUtils.doKerberosAuthentication(JdbcConnectionConfig.of(options));
            }
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }
}
