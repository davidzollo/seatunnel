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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlite;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite.SqliteTypeMapper;

import com.mysql.cj.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SqliteCatalog extends AbstractJdbcCatalog {

    protected static final Set<String> SYS_DATABASES = new HashSet<>(4);

    public SqliteCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        // because sqlite no need username
        super(catalogName, "username", pwd, urlInfo, null);
    }

    @Override
    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {

        return true;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        // sqlite nonsupport update databaseName
        Connection connection = getConnection(defaultUrl);
        try (PreparedStatement ps =
                        connection.prepareStatement(
                                "select name from sqlite_master where type = \"table\";");
                ResultSet rs = ps.executeQuery()) {
            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "select * from sqlite_master where type = \"table\" and name = \"%s\";",
                tablePath.getTableName());
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {

        String columnName = resultSet.getString("COLUMN_NAME");
        String fullTypeName = resultSet.getString("TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_SIZE");
        long columnScale = resultSet.getLong("DECIMAL_DIGITS");
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = resultSet.getInt("NULLABLE") == 1;

        String dataType = JDBCType.valueOf(resultSet.getInt("DATA_TYPE")).getName();
        SeaTunnelDataType<?> type = fromJdbcType(columnName, dataType, columnLength, columnScale);

        return PhysicalColumn.of(
                columnName,
                type,
                0,
                isNullable,
                defaultValue,
                columnComment,
                fullTypeName,
                false,
                false,
                0L,
                null,
                columnLength);
    }

    public static Map<String, Object> getColumnsDefaultValue(TablePath tablePath, Connection conn) {
        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getTableName(), "`", false));
        queryBuf.append(" FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getDatabaseName(), "`", false));
        try (PreparedStatement ps2 = conn.prepareStatement(queryBuf.toString());
                ResultSet rs = ps2.executeQuery()) {
            Map<String, Object> result = new HashMap<>();
            while (rs.next()) {
                String field = rs.getString("Field");
                Object defaultValue = rs.getObject("Default");
                result.put(field, defaultValue);
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting table(%s) columns default value",
                            tablePath.getFullName()),
                    e);
        }
    }

    // todo: If the origin source is mysql, we can directly use create table like to create the
    @Override
    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        //
        //        String createTableSql =
        //                MysqlCreateTableSqlBuilder.builder(tablePath,
        // table).build(table.getCatalogName());
        //        createTableSql =
        //                CatalogUtils.getFieldIde(createTableSql,
        // table.getOptions().get("fieldIde"));
        //        Connection connection = getConnection(defaultUrl);
        //        log.info("create table sql: {}", createTableSql);
        //        try (PreparedStatement ps = connection.prepareStatement(createTableSql)) {
        //            ps.execute();
        //        } catch (Exception e) {
        //            throw new CatalogException(
        //                    String.format("Failed creating table %s", tablePath.getFullName()),
        // e);
        //        }
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE IF EXISTS `%s`;", tablePath.getTableName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format("DELETE FROM `%s`;", tablePath.getTableName());
    }

    @Override
    protected void createDatabaseInternal(String databaseName) throws CatalogException {}

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {}

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select * from `%s` LIMIT 1;", tablePath.getTableName());
    }

    private SeaTunnelDataType<?> fromJdbcType(
            String columnName, String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(MysqlDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(MysqlDataTypeConvertor.SCALE, scale);
        return new SqliteDataTypeConvertor()
                .toSeaTunnelType(columnName, typeName, dataTypeProperties);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new SqliteTypeMapper());
    }
}
