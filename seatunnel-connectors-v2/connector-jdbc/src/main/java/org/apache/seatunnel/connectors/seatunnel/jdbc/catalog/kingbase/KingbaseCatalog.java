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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase8.KingbaseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase8.KingbaseTypeMapper;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KingbaseCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseCatalog.class);

    public static final Set<String> KINGBASE_SYSTEM_DATABASES =
            Sets.newHashSet("TEMPLATE1", "TEMPLATE0", "TEMPLATE2", "SAMPLES", "SECURITY");

    public static final Set<String> KINGBASE_SYSTEM_SCHEMAS =
            Sets.newHashSet(
                    "information_schema",
                    "SYS_CATALOG",
                    "SYSAUDIT",
                    "SYS_HM",
                    "SYS_TOAST",
                    "SYS_TEMP_1",
                    "SYSLOGICAL",
                    "SYS_TOAST_TEMP_1");

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypmod = -1 THEN t.typname\n"
                    + "        WHEN t.typname = 'varchar' AND a.atttypmod < -1 THEN t.typname || '(' || ((a.atttypmod * -1) - 4) || ')'\n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' AND a.atttypmod < -1 THEN 'CHAR' || '(' || ((a.atttypmod * -1) - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'CHAR' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || a.atttypmod || ')'\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN t.typname || '(' || a.atttypmod || ')'\n"
                    + "        ELSE t.typname || '' \n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar') AND a.atttypmod < -1 THEN (a.atttypmod * -1) - 4\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar') THEN a.atttypmod - 4\n"
                    + "        WHEN t.typname IN ('bit', 'bit varying') THEN a.atttypmod\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) >> 16\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_length,\n"
                    + "\t\tCASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN a.atttypmod\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_scale,\n"
                    + "\t\td.description AS column_comment,\n"
                    + "\t\tnull AS default_value,\n"
                    + "\t\tCASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable\n"
                    + "FROM \n"
                    + "    sys_class c\n"
                    + "    JOIN sys_namespace n ON c.relnamespace = n.oid\n"
                    + "    JOIN sys_attribute a ON a.attrelid = c.oid\n"
                    + "    JOIN sys_type t ON a.atttypid = t.oid\n"
                    + "    LEFT JOIN sys_description d ON c.oid = d.objoid AND a.attnum = d.objsubid\n"
                    + "    LEFT JOIN sys_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
                    + "WHERE \n"
                    + "    n.nspname = '%s'\n"
                    + "    AND c.relname = '%s'\n"
                    + "    AND a.attnum > 0\n"
                    + "ORDER BY \n"
                    + "    a.attnum;";
    protected final Map<String, Connection> connectionMap;

    public KingbaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select * from %s LIMIT 1;", tablePath.getFullName());
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new KingbaseCreateTableSqlBuilder(table)
                .build(tablePath, table.getOptions().get("fieldIde"));
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        return "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + tableName + "\"";
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        return "TRUNCATE TABLE  \"" + schemaName + "\".\"" + tableName + "\"";
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format("select datname from sys_database WHERE datname = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
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
    public List<String> listDatabases() throws CatalogException {
        List<String> dbNames = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement statement =
                        connection.prepareStatement("select datname from sys_database;");
                ResultSet re = statement.executeQuery()) {
            while (re.next()) {
                String dbName = re.getString("datname");
                if (StringUtils.isNotBlank(dbName) && !KINGBASE_SYSTEM_DATABASES.contains(dbName)) {
                    dbNames.add(dbName);
                }
            }
            return dbNames;
        } catch (Exception e) {
            throw new CatalogException("get databases failed", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tableNames = new ArrayList<>();
        String query = "SELECT table_schema, table_name FROM information_schema.tables";
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try (Connection connection = DriverManager.getConnection(dbUrl, username, pwd);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("table_schema");
                String tableName = resultSet.getString("table_name");
                if (StringUtils.isNotBlank(schemaName)
                        && !KINGBASE_SYSTEM_SCHEMAS.contains(schemaName)) {
                    tableNames.add(schemaName + "." + tableName);
                }
            }
            return tableNames;
        } catch (Exception e) {
            throw new CatalogException("get table names failed", e);
        }
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name");
        String fullTypeName = resultSet.getString("full_type_name");
        long columnLength = resultSet.getLong("column_length");
        int columnScale = resultSet.getInt("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("YES");
        // dealingSpecialNumeric
        if (typeName.equals(KingbaseTypeConverter.PG_NUMERIC) && columnLength < 1) {
            fullTypeName = "numeric(38,10)";
            columnLength = 38;
            columnScale = 10;
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return KingbaseTypeConverter.INSTANCE.convert(typeDefine);
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new KingbaseTypeMapper());
    }
}
