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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT \n"
                    + "    n.nspname as schema_name, \n"
                    + "    c.relname as table_name, \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypmod = -1 THEN t.typname\n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'char' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN t.typname || '(' || a.atttypmod || ')'\n"
                    + "        ELSE t.typname || '' \n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying') THEN a.atttypmod - 4\n"
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
                    + "\t\tpg_get_expr(ad.adbin, ad.adrelid) AS default_value,\n"
                    + "\t\tCASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable\n"
                    + "FROM \n"
                    + "    pg_class c\n"
                    + "    JOIN pg_namespace n ON c.relnamespace = n.oid\n"
                    + "    JOIN pg_attribute a ON a.attrelid = c.oid\n"
                    + "    JOIN pg_type t ON a.atttypid = t.oid\n"
                    + "    LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid\n"
                    + "    LEFT JOIN pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
                    + "WHERE \n"
                    + "    n.nspname = '%s'\n"
                    + "    AND c.relname = '%s'\n"
                    + "    AND a.attnum > 0\n"
                    + "ORDER BY \n"
                    + "    a.attnum";

    protected static final String SELECT_COLUMNS_SQL_TEMPLATE_FOR_POSTGIS =
            "SELECT\n"
                    + "    st_tmp1.column_name,\n"
                    + "    st_tmp1.type_name,\n"
                    + "    (CASE WHEN st_tmp1.type_name IN ('geometry') THEN st_tmp1.type_name || '(' || geometry.type || CASE WHEN geometry.srid != 0 THEN ',' || geometry.srid ELSE '' END ||  ')'\n"
                    + "          WHEN st_tmp1.type_name IN ('geography') THEN st_tmp1.type_name || '(' || geography.type || CASE WHEN geography.srid != 0 THEN ',' || geography.srid ELSE '' END || ')'\n"
                    + "          ELSE st_tmp1.full_type_name END) AS full_type_name,\n"
                    + "    st_tmp1.column_length,\n"
                    + "    st_tmp1.column_scale,\n"
                    + "    st_tmp1.column_comment,\n"
                    + "    st_tmp1.default_value,\n"
                    + "    st_tmp1.is_nullable\n"
                    + "       FROM (%s) st_tmp1\n"
                    + "    LEFT JOIN %s.geometry_columns geometry ON st_tmp1.schema_name = geometry.f_table_schema AND st_tmp1.table_name = geometry.f_table_name AND st_tmp1.column_name = geometry.f_geometry_column\n"
                    + "    LEFT JOIN %s.geography_columns geography ON st_tmp1.schema_name = geography.f_table_schema AND st_tmp1.table_name = geography.f_table_name AND st_tmp1.column_name = geography.f_geography_column\n";

    private static final String QUERY_PLUGINS = "SELECT extname FROM pg_extension";

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("pg_catalog");
        SYS_DATABASES.add("root");
        SYS_DATABASES.add("pg_toast");
        SYS_DATABASES.add("pg_temp_1");
        SYS_DATABASES.add("pg_toast_temp_1");
        SYS_DATABASES.add("postgres");
        SYS_DATABASES.add("template0");
        SYS_DATABASES.add("template1");
    }

    public PostgresCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where datname = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where table_schema = '%s' and table_name= '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "select datname from pg_database";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT table_schema, table_name FROM information_schema.tables";
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
            Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
            List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
            constraintKeys = filterDuplicateConstraintKeys(constraintKeys, primaryKey);
            String tableComment = getTableComment(metaData, tablePath);
            try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
                    ResultSet resultSet = ps.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // get column names
                final List<Column> columns = builder.build().getColumns();
                final List<String> columnNameList =
                        columns.stream().map(Column::getName).collect(Collectors.toList());
                // filter some not exit column
                final List<ConstraintKey> finalConstraintKeys =
                        constraintKeys.stream()
                                .filter(
                                        constraintKey -> {
                                            final List<ConstraintKey.ConstraintKeyColumn>
                                                    constraintKeyColumnNames =
                                                            constraintKey.getColumnNames();
                                            List<String> constraintKeyColumnNameStrList =
                                                    constraintKeyColumnNames.stream()
                                                            .map(
                                                                    ConstraintKey
                                                                                    .ConstraintKeyColumn
                                                                            ::getColumnName)
                                                            .collect(Collectors.toList());
                                            boolean isCanUniqueKey = true;
                                            for (String constraintKeyColumnName :
                                                    constraintKeyColumnNameStrList) {
                                                if (!columnNameList.contains(
                                                        constraintKeyColumnName)) {
                                                    isCanUniqueKey = false;
                                                    break;
                                                }
                                            }
                                            return isCanUniqueKey
                                                    && constraintKey.getConstraintType()
                                                            == ConstraintKey.ConstraintType
                                                                    .UNIQUE_KEY;
                                        })
                                .collect(Collectors.toList());

                finalConstraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        tableComment,
                        catalogName);
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        Collection<String> plugins = plugins(tablePath);
        String selectColumnsSQL =
                String.format(
                        SELECT_COLUMNS_SQL_TEMPLATE,
                        tablePath.getSchemaName(),
                        tablePath.getTableName());
        if (plugins.contains(PostgresTypeConverter.PG_POSTGIS)) {
            String postgisPath = selectPostgisPath(tablePath);
            return String.format(
                    SELECT_COLUMNS_SQL_TEMPLATE_FOR_POSTGIS,
                    selectColumnsSQL,
                    postgisPath,
                    postgisPath);
        }
        return selectColumnsSQL;
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
        if (typeName.equals(PostgresTypeConverter.PG_NUMERIC) && columnLength < 1) {
            fullTypeName = "numeric(38,10)";
            columnLength = 38;
            columnScale = 10;
        }
        if (defaultValue != null && defaultValue.toString().contains("regclass")) {
            defaultValue = null;
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
        return getTypeConverter().convert(typeDefine);
    }

    public TypeConverter getTypeConverter() {
        return PostgresTypeConverter.INSTANCE;
    }

    @Override
    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                createTableSqlBuilder(tablePath, table);
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            String createTableSql = postgresCreateTableSqlBuilder.build(tablePath);
            executeInternal(dbUrl, createTableSql);

            if (postgresCreateTableSqlBuilder.isHaveConstraintKey) {
                String alterTableSql =
                        "ALTER TABLE "
                                + tablePath.getSchemaAndTableName("\"")
                                + " REPLICA IDENTITY FULL;";
                executeInternal(dbUrl, alterTableSql);
            }

            if (CollectionUtils.isNotEmpty(postgresCreateTableSqlBuilder.getCreateIndexSqls())) {
                for (String createIndexSql : postgresCreateTableSqlBuilder.getCreateIndexSqls()) {
                    executeInternal(dbUrl, createIndexSql);
                }
            }

        } catch (Exception ex) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), ex);
        }
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                createTableSqlBuilder(tablePath, table);
        return postgresCreateTableSqlBuilder.build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return "DROP TABLE \""
                + tablePath.getSchemaName()
                + "\".\""
                + tablePath.getTableName()
                + "\"";
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return "CREATE DATABASE \"" + databaseName + "\"";
    }

    public String getExistDataSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return String.format("select * from \"%s\".\"%s\" limit 1", schemaName, tableName);
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return "TRUNCATE TABLE  \"" + schemaName + "\".\"" + tableName + "\"";
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
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new PostgresTypeMapper());
    }

    protected Collection<String> plugins(TablePath tablePath) {
        String dbUrl = getJdbcURL(tablePath);
        try {
            Connection connection = getConnection(dbUrl);
            Set<String> plugins = new HashSet<>();
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(QUERY_PLUGINS)) {
                while (rs.next()) {
                    plugins.add(rs.getString(1));
                }
                return plugins;
            }
        } catch (Exception e) {
            log.error("Failed to load plugins", e);
            return Collections.emptyList();
        }
    }

    protected String selectPostgisPath(TablePath tablePath) {
        String dbUrl = getJdbcURL(tablePath);
        String sql = "SELECT schemaname FROM pg_views WHERE viewname = 'geometry_columns'";
        Connection conn = getConnection(dbUrl);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed execute query %s", sql), e);
        }
    }

    protected PostgresCreateTableSqlBuilder createTableSqlBuilder(
            TablePath tablePath, CatalogTable table) {
        return new PostgresCreateTableSqlBuilder(table, plugins(tablePath));
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
