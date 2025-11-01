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
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeMapper.SNOWFLAKE_TIMESTAMP_LTZ;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeMapper.SNOWFLAKE_TIMESTAMP_NTZ;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeMapper.SNOWFLAKE_TIMESTAMP_TZ;

@Slf4j
public class SnowflakeCatalog extends AbstractJdbcCatalog {

    SnowflakeDriver driver;

    public SnowflakeCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    protected Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Properties info = new Properties();
            info.put("user", username);
            if (StringUtils.isNotBlank(pwd)) {
                info.put("password", pwd);
            }
            Connection connection = driver.connect(url, info);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public void open() throws CatalogException {
        this.driver = new SnowflakeDriver();
        this.getConnection(defaultUrl);
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT 1 FROM \"%s\".\"%s\".\"%s\" LIMIT 1",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES";
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        String schemaName = defaultSchema.orElse("PUBLIC");

        try (Connection connection = getConnection(dbUrl);
                PreparedStatement ps =
                        connection.prepareStatement(
                                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?")) {

            ps.setString(1, schemaName);
            List<String> tables = new ArrayList<>();

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tables.add(tableName);
                }
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed listing tables in catalog %s database %s",
                            catalogName, databaseName),
                    e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        Connection conn = getConnection(dbUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            constraintKeys = filterDuplicateConstraintKeys(constraintKeys, primaryKey);
            String tableComment = getTableComment(metaData, tablePath);
            try (ResultSet resultSet =
                    metaData.getColumns(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            null)) {
                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        tableComment,
                        catalogName);
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = normalizeTypeName(resultSet.getString("TYPE_NAME"));
        long columnLength = resultSet.getLong("COLUMN_SIZE");
        int columnScale = resultSet.getInt("DECIMAL_DIGITS");
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = resultSet.getInt("NULLABLE") == 1;

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return SnowflakeTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new SnowflakeCreateTableSqlBuilder(table)
                .build(
                        "\""
                                + tablePath.getDatabaseName()
                                + "\".\""
                                + tablePath.getSchemaName()
                                + "\".\""
                                + tablePath.getTableName()
                                + "\"");
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return "DROP TABLE IF EXISTS \""
                + tablePath.getDatabaseName()
                + "\".\""
                + tablePath.getSchemaName()
                + "\".\""
                + tablePath.getTableName()
                + "\"";
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\".\"%s\"",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " LIKE '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return "CREATE DATABASE \"" + databaseName + "\"";
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return "DROP DATABASE IF EXISTS \"" + databaseName + "\"";
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(defaultUrl), sqlQuery, new SnowflakeTypeMapper());
    }

    private String normalizeTypeName(String rawTypeName) {
        if (rawTypeName == null) {
            return null;
        }
        switch (rawTypeName.toUpperCase(java.util.Locale.ROOT)) {
            case "TIMESTAMPNTZ":
                return SNOWFLAKE_TIMESTAMP_NTZ;
            case "TIMESTAMPLTZ":
                return SNOWFLAKE_TIMESTAMP_LTZ;
            case "TIMESTAMPTZ":
                return SNOWFLAKE_TIMESTAMP_TZ;
            default:
                return rawTypeName;
        }
    }
}
