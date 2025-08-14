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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlserverTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    public static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT tbl.name AS table_name,\n"
                    + "       col.name AS column_name,\n"
                    + "       ext.value AS comment,\n"
                    + "       col.column_id AS column_id,\n"
                    + "       types.name AS type,\n"
                    + "       col.max_length AS max_length,\n"
                    + "       col.precision AS precision,\n"
                    + "       col.scale AS scale,\n"
                    + "       col.is_nullable AS is_nullable,\n"
                    + "       def.definition AS default_value\n"
                    + "FROM sys.objects tbl\n"
                    + "    INNER JOIN sys.columns col ON tbl.object_id = col.object_id\n"
                    + "    LEFT JOIN sys.types types ON col.system_type_id = types.user_type_id\n"
                    + "    LEFT JOIN sys.extended_properties ext ON ext.major_id = col.object_id AND ext.minor_id = col.column_id\n"
                    + "    LEFT JOIN sys.default_constraints def ON col.default_object_id = def.object_id AND ext.minor_id = col.column_id AND ext.name = 'MS_Description'\n"
                    + "WHERE schema_name(tbl.schema_id) = '%s' %s\n"
                    + "ORDER BY tbl.name, col.column_id";

    public SqlServerCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + "  where name = N'%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + "  and  TABLE_SCHEMA = N'%s' and TABLE_NAME = N'%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT NAME FROM sys.databases";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM [%s].[INFORMATION_SCHEMA].[TABLES] WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')",
                databaseName);
    }

    @Override
    public List<CatalogTable> getTables(ReadonlyConfig config) throws CatalogException {
        return getTables(config, this::getTable);
    }

    @Override
    public List<CatalogTable> getTables(
            ReadonlyConfig config, CatalogTableFunction<TablePath> getTableFunction)
            throws CatalogException {
        // Get the list of specified tables
        List<String> tableNames = config.get(CatalogOptions.TABLE_NAMES);
        if (tableNames != null && !tableNames.isEmpty()) {
            Iterator<TablePath> tablePaths =
                    tableNames.stream()
                            .map(
                                    fullTableName -> {
                                        if (!fullTableName.contains("].[")) {
                                            return TablePath.of(fullTableName, true);
                                        } else {
                                            String[] parts =
                                                    fullTableName
                                                            .substring(
                                                                    1, fullTableName.length() - 1)
                                                            .split("\\]\\.\\[");
                                            String databaseName = parts[0];
                                            String schemaName = parts[1];
                                            String tableName = parts[2];
                                            return TablePath.of(
                                                    databaseName, schemaName, tableName);
                                        }
                                    })
                            .filter(this::tableExists)
                            .iterator();
            return buildCatalogTablesWithErrorCheck(tablePaths, getTableFunction);
        }

        // Get the list of table pattern
        String tablePatternStr = config.get(CatalogOptions.TABLE_PATTERN);
        if (StringUtils.isBlank(tablePatternStr)) {
            return Collections.emptyList();
        }
        Pattern databasePattern = Pattern.compile(config.get(CatalogOptions.DATABASE_PATTERN));
        Pattern tablePattern = Pattern.compile(config.get(CatalogOptions.TABLE_PATTERN));
        List<String> allDatabase = this.listDatabases();
        allDatabase.removeIf(s -> !databasePattern.matcher(s).matches());
        List<TablePath> tablePaths = new ArrayList<>();
        for (String databaseName : allDatabase) {
            tableNames = this.listTables(databaseName);
            tableNames.forEach(
                    tableName -> {
                        if (tablePattern.matcher(databaseName + "." + tableName).matches()) {
                            tablePaths.add(TablePath.of(databaseName, tableName));
                        }
                    });
        }
        return buildCatalogTablesWithErrorCheck(tablePaths.iterator(), getTableFunction);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        String tableSql =
                StringUtils.isNotEmpty(tablePath.getTableName())
                        ? "AND tbl.name = N'" + tablePath.getTableName() + "'"
                        : "";

        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tableSql);
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String dataType = resultSet.getString("type");
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getBoolean("is_nullable");
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision((long) precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return SqlServerTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return SqlServerCreateTableSqlBuilder.builder(tablePath, table).build(tablePath, table);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE [%s]", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE IF EXISTS [%s];", databaseName);
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SqlserverTypeMapper());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select TOP 1 * from %s ;", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format("TRUNCATE TABLE  %s", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        String sql =
                "select\n"
                        + "  p.value\n"
                        + "from\n"
                        + "  sys.tables t\n"
                        + "  join sys.extended_properties p on t.object_id = p.major_id\n"
                        + "  and p.minor_id = '0'\n"
                        + "  and schema_name(t.schema_id) = ? \n"
                        + "  and t.name = ?";
        try (PreparedStatement statement =
                getConnection(getJdbcURL(tablePath)).prepareStatement(sql)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                return null;
            }
        } catch (SQLException e) {
            log.warn("Failed to get table comment", e);
            return null;
        }
    }

    @Override
    protected String getJdbcURL(TablePath tablePath) {
        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        return dbUrl;
    }
}
