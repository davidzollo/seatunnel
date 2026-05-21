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
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KingbaseCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseCatalog.class);
    private static final String PG_CATALOG_PROBE_SQL = "SELECT 1 FROM pg_class LIMIT 1";
    private static final String PG_DATABASE_EXISTS_SQL_TEMPLATE =
            "select datname from pg_database WHERE datname = '%s'";
    private static final String SYS_DATABASE_EXISTS_SQL_TEMPLATE =
            "select datname from sys_database WHERE datname = '%s'";
    private static final String LIST_PG_DATABASES_SQL = "select datname from pg_database;";
    private static final String LIST_SYS_DATABASES_SQL = "select datname from sys_database;";
    private static final String SELECT_PG_TABLE_COMMENT_SQL =
            "SELECT obj_description(c.oid, 'pg_class') "
                    + "FROM pg_class c "
                    + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                    + "WHERE n.nspname = ? AND c.relname = ?";
    private static final String SELECT_SYS_TABLE_COMMENT_SQL =
            "SELECT d.description "
                    + "FROM sys_class c "
                    + "JOIN sys_namespace n ON c.relnamespace = n.oid "
                    + "LEFT JOIN sys_description d ON d.objoid = c.oid AND d.objsubid = 0 "
                    + "WHERE n.nspname = ? AND c.relname = ?";

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

    // Kingbase V9R1C10 exposes PostgreSQL-compatible pg_* catalogs in PostgreSQL mode.
    private static final String SELECT_COLUMNS_PG_SQL_TEMPLATE =
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

    private static final String SELECT_COLUMNS_SYS_SQL_TEMPLATE =
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

    // Query primary keys directly from pg catalogs because Kingbase V8/V9 JDBC metadata
    // can call information_schema helpers that are absent in some server builds.
    private static final String SELECT_PRIMARY_KEYS_SQL =
            "SELECT con.conname AS pk_name, a.attname AS column_name, ord.key_seq AS key_seq "
                    + "FROM pg_constraint con "
                    + "JOIN pg_class cls ON cls.oid = con.conrelid "
                    + "JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN unnest(con.conkey::int[]) WITH ORDINALITY AS ord(attnum, key_seq) ON TRUE "
                    + "JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE con.contype = 'p' AND nsp.nspname = ? AND cls.relname = ? "
                    + "ORDER BY ord.key_seq";

    // Query secondary indexes directly from pg catalogs because Kingbase V8/V9 JDBC metadata can
    // still call missing information_schema helper functions even after primary key lookup is
    // customized.
    private static final String SELECT_CONSTRAINT_KEYS_SQL =
            "SELECT idx.relname AS index_name, ind.indisunique AS is_unique, "
                    + "a.attname AS column_name, ord.key_seq AS key_seq "
                    + "FROM pg_index ind "
                    + "JOIN pg_class cls ON cls.oid = ind.indrelid "
                    + "JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN pg_class idx ON idx.oid = ind.indexrelid "
                    + "JOIN unnest(ind.indkey::int[]) WITH ORDINALITY AS ord(attnum, key_seq) ON TRUE "
                    + "JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE nsp.nspname = ? AND cls.relname = ? AND ind.indisprimary = false "
                    + "ORDER BY idx.relname, ord.key_seq";

    private volatile boolean usePostgresCatalog = true;

    public KingbaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    public void open() throws CatalogException {
        super.open();
        usePostgresCatalog = probePostgresCatalog();
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
        return String.format(
                usePostgresCatalog
                        ? PG_DATABASE_EXISTS_SQL_TEMPLATE
                        : SYS_DATABASE_EXISTS_SQL_TEMPLATE,
                databaseName);
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
        try (PreparedStatement statement =
                        getConnection(defaultUrl)
                                .prepareStatement(
                                        usePostgresCatalog
                                                ? LIST_PG_DATABASES_SQL
                                                : LIST_SYS_DATABASES_SQL);
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
        try (Statement statement = getConnection(dbUrl).createStatement();
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
                usePostgresCatalog
                        ? SELECT_COLUMNS_PG_SQL_TEMPLATE
                        : SELECT_COLUMNS_SYS_SQL_TEMPLATE,
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

    @Override
    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        if (!usePostgresCatalog) {
            return super.getPrimaryKey(metaData, tablePath);
        }
        String databaseName =
                StringUtils.isNotBlank(tablePath.getDatabaseName())
                        ? tablePath.getDatabaseName()
                        : defaultDatabase;
        Connection connection = getConnection(getUrlFromDatabaseName(databaseName));
        try (PreparedStatement statement = connection.prepareStatement(SELECT_PRIMARY_KEYS_SQL)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                String primaryKeyName = null;
                Map<Integer, String> orderedColumns = new LinkedHashMap<>();
                while (resultSet.next()) {
                    if (primaryKeyName == null) {
                        primaryKeyName = resultSet.getString("pk_name");
                    }
                    orderedColumns.put(
                            resultSet.getInt("key_seq"), resultSet.getString("column_name"));
                }
                if (primaryKeyName == null || orderedColumns.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(
                        PrimaryKey.of(primaryKeyName, new ArrayList<>(orderedColumns.values())));
            }
        }
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        if (!usePostgresCatalog) {
            return super.getConstraintKeys(metaData, tablePath);
        }
        String databaseName =
                StringUtils.isNotBlank(tablePath.getDatabaseName())
                        ? tablePath.getDatabaseName()
                        : defaultDatabase;
        Connection connection = getConnection(getUrlFromDatabaseName(databaseName));
        try (PreparedStatement statement =
                connection.prepareStatement(SELECT_CONSTRAINT_KEYS_SQL)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                Map<String, ConstraintKey> constraintKeyMap = new LinkedHashMap<>();
                while (resultSet.next()) {
                    String indexName = resultSet.getString("index_name");
                    boolean unique = resultSet.getBoolean("is_unique");
                    ConstraintKey constraintKey =
                            constraintKeyMap.computeIfAbsent(
                                    indexName,
                                    key ->
                                            ConstraintKey.of(
                                                    unique
                                                            ? ConstraintKey.ConstraintType
                                                                    .UNIQUE_KEY
                                                            : ConstraintKey.ConstraintType
                                                                    .INDEX_KEY,
                                                    key,
                                                    new ArrayList<>()));
                    constraintKey
                            .getColumnNames()
                            .add(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            resultSet.getString("column_name"),
                                            ConstraintKey.ColumnSortType.ASC));
                }
                return new ArrayList<>(constraintKeyMap.values());
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to query constraint keys from pg catalogs for table {}, returning empty list.",
                    tablePath,
                    e);
            return new ArrayList<>();
        }
    }

    @Override
    protected String getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        String databaseName =
                StringUtils.isNotBlank(tablePath.getDatabaseName())
                        ? tablePath.getDatabaseName()
                        : defaultDatabase;
        String commentSql =
                usePostgresCatalog ? SELECT_PG_TABLE_COMMENT_SQL : SELECT_SYS_TABLE_COMMENT_SQL;
        try (PreparedStatement statement =
                getConnection(getUrlFromDatabaseName(databaseName)).prepareStatement(commentSql)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                return null;
            }
        } catch (SQLException e) {
            LOG.warn("Failed to query table comment for {}", tablePath, e);
            return null;
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new KingbaseTypeMapper());
    }

    private boolean probePostgresCatalog() {
        try (Statement statement = getConnection(defaultUrl).createStatement();
                ResultSet ignored = statement.executeQuery(PG_CATALOG_PROBE_SQL)) {
            return true;
        } catch (SQLException e) {
            LOG.info(
                    "KingbaseCatalog falling back to legacy sys_* catalogs because pg_* compatibility probe failed: {}",
                    e.getMessage());
            return false;
        }
    }
}
