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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.gaussdb;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gaussdb.GaussDBTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gaussdb.GaussDBTypeMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class GaussDBCatalog extends PostgresCatalog {
    // added nvarchar2 type for gaussdb
    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN a.atttypmod = -1 THEN t.typname\n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'nvarchar2' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'char' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz') THEN t.typname || '(' || a.atttypmod || ')'\n"
                    + "        ELSE t.typname || '' \n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN a.atttypmod = -1 THEN NULL\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying', 'nvarchar2') THEN a.atttypmod - 4\n"
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
                    + "    a.attnum;";

    public GaussDBCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected void createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        GaussDBCreateTableSqlBuilder gaussDBCreateTableSqlBuilder =
                createTableSqlBuilder(tablePath, table);
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try {
            List<String> createTableSqls =
                    gaussDBCreateTableSqlBuilder.buildGaussDBCreateTableSql(tablePath);
            for (String sql : createTableSqls) {
                executeInternal(dbUrl, sql);
            }

            // Execute index creation statements
            if (CollectionUtils.isNotEmpty(gaussDBCreateTableSqlBuilder.getCreateIndexSqls())) {
                for (String createIndexSql : gaussDBCreateTableSqlBuilder.getCreateIndexSqls()) {
                    executeInternal(dbUrl, createIndexSql);
                }
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }

    public GaussDBCreateTableSqlBuilder createTableSqlBuilder(
            TablePath tablePath, CatalogTable table) {
        return new GaussDBCreateTableSqlBuilder(table);
    }

    @Override
    public TypeConverter getTypeConverter() {
        return GaussDBTypeConverter.INSTANCE;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new GaussDBTypeMapper());
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
