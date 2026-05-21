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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class RedshiftCatalog extends AbstractJdbcCatalog {

    /**
     * Late-binding views are invisible to the pg_attribute path used by ordinary Redshift tables
     * and schema-bound views, so keep the existing query first and fall back to svv_columns only
     * when pg_catalog returns no columns.
     */
    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "WITH pg_columns AS (\n"
                    + "    SELECT \n"
                    + "        a.attname AS column_name, \n"
                    + "        t.typname AS type_name, \n"
                    + "        pg_catalog.format_type(a.atttypid, a.atttypmod) AS full_type_name, \n"
                    + "        CASE \n"
                    + "            WHEN a.atttypmod = -1 THEN NULL \n"
                    + "            WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying') THEN a.atttypmod - 4 \n"
                    + "            WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) >> 16 \n"
                    + "            ELSE NULL \n"
                    + "        END AS column_length, \n"
                    + "        CASE \n"
                    + "            WHEN a.atttypmod = -1 THEN NULL \n"
                    + "            WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535 \n"
                    + "            ELSE NULL \n"
                    + "        END AS column_scale, \n"
                    + "        pg_catalog.col_description(a.attrelid, a.attnum) AS column_comment, \n"
                    + "        pg_get_expr(ad.adbin, ad.adrelid) AS default_value, \n"
                    + "        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, \n"
                    + "        a.attnum AS ordinal_position \n"
                    + "    FROM \n"
                    + "        pg_class c \n"
                    + "        JOIN pg_namespace n ON c.relnamespace = n.oid \n"
                    + "        JOIN pg_attribute a ON a.attrelid = c.oid \n"
                    + "        JOIN pg_type t ON a.atttypid = t.oid \n"
                    + "        LEFT JOIN pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid \n"
                    + "    WHERE \n"
                    + "        n.nspname = '%s' \n"
                    + "        AND c.relname = '%s' \n"
                    + "        AND a.attnum > 0 \n"
                    + "), late_binding_columns AS (\n"
                    + "    SELECT \n"
                    + "        column_name AS column_name, \n"
                    + "        data_type AS type_name, \n"
                    + "        data_type AS full_type_name, \n"
                    + "        COALESCE(CAST(numeric_precision AS BIGINT), CAST(character_maximum_length AS BIGINT)) AS column_length, \n"
                    + "        numeric_scale AS column_scale, \n"
                    + "        remarks AS column_comment, \n"
                    + "        column_default AS default_value, \n"
                    + "        is_nullable AS is_nullable, \n"
                    + "        ordinal_position AS ordinal_position \n"
                    + "    FROM svv_columns \n"
                    + "    WHERE table_catalog = '%s' \n"
                    + "      AND table_schema = '%s' \n"
                    + "      AND table_name = '%s' \n"
                    + ")\n"
                    + "SELECT \n"
                    + "    column_name, \n"
                    + "    type_name, \n"
                    + "    full_type_name, \n"
                    + "    column_length, \n"
                    + "    column_scale, \n"
                    + "    column_comment, \n"
                    + "    default_value, \n"
                    + "    is_nullable, \n"
                    + "    ordinal_position \n"
                    + "FROM pg_columns \n"
                    + "UNION ALL \n"
                    + "SELECT \n"
                    + "    column_name, \n"
                    + "    type_name, \n"
                    + "    full_type_name, \n"
                    + "    column_length, \n"
                    + "    column_scale, \n"
                    + "    column_comment, \n"
                    + "    default_value, \n"
                    + "    is_nullable, \n"
                    + "    ordinal_position \n"
                    + "FROM late_binding_columns \n"
                    + "WHERE NOT EXISTS (SELECT 1 FROM pg_columns) \n"
                    + "ORDER BY ordinal_position";

    public RedshiftCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String schema) {
        super(catalogName, username, pwd, urlInfo, schema);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where datname = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where table_schema = '%s' and table_name = '%s'",
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
    protected String getTableName(ResultSet rs) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        return stringBuilder
                .append(rs.getString(1))
                .append(".")
                .append(rs.getString(2))
                .toString()
                .toLowerCase();
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        String createTableSql =
                new RedshiftCreateTableSqlBuilder(table)
                        .build(tablePath, table.getOptions().get("fieldIde"));
        return CatalogUtils.getFieldIde(createTableSql, table.getOptions().get("fieldIde"));
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE %s;", tablePath.getSchemaName() + "." + tablePath.getTableName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE %s;", tablePath.getSchemaName() + "." + tablePath.getTableName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE,
                tablePath.getSchemaName().toLowerCase(),
                tablePath.getTableName().toLowerCase(),
                tablePath.getDatabaseName().toLowerCase(),
                tablePath.getSchemaName().toLowerCase(),
                tablePath.getTableName().toLowerCase());
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
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
        String isNullableStr = resultSet.getString("is_nullable");
        boolean isNullable = isNullableStr.equals("YES");

        log.info(
                "Redshift buildColumn - columnName: {}, typeName: {}, fullTypeName: {}, columnComment: '{}', defaultValue: {}",
                columnName,
                typeName,
                fullTypeName,
                columnComment,
                defaultValue);

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

        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);

        return column;
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select * from %s LIMIT 1;", tablePath.getFullName());
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new RedshiftTypeMapper());
    }
}
