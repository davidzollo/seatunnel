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

package org.apache.seatunnel.connectors.dws.guassdb.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.dws.guassdb.config.DwsGaussDBConfig;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.huawei.gauss200.jdbc.core.BaseConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.DATABASE;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.PASSWORD;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.TABLE;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.URL;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.USER;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.COPY_FILE_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.FIELD_IDE;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.PRIMARY_KEY;

@Slf4j
public class DwsGaussDBCatalog implements Catalog, Serializable {

    private static final String SELECT_COLUMNS_SQL =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'char' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        ELSE t.typname\n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying') THEN a.atttypmod - 4\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) >> 16\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_length,\n"
                    + "\t\tCASE\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535\n"
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

    private Set<String> SYS_DATABASES =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("pg_catalog");
                    add("root");
                    add("pg_toast");
                    add("pg_temp_1");
                    add("pg_toast_temp_1");
                    add("postgres");
                    add("template0");
                    add("template1");
                }
            };

    private final String catalogName;

    private String defaultDatabase;
    private String username;
    private String password;
    private String baseUrl;
    private String defaultUrl;
    private JdbcUrlUtil.UrlInfo urlInfo;
    private ReadonlyConfig readonlyConfig;
    private Map<String, String> jdbcProperties;

    private Map<String, BaseConnection> connectionMap;

    public DwsGaussDBCatalog(
            String catalogName,
            String username,
            String password,
            JdbcUrlUtil.UrlInfo urlInfo,
            Map<String, String> jdbcProperties,
            String defaultSchema,
            ReadonlyConfig readonlyConfig) {
        this.catalogName = catalogName;
        this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        this.username = username;
        this.password = password;
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        this.defaultUrl = urlInfo.getOrigin();
        this.urlInfo = urlInfo;
        this.jdbcProperties = jdbcProperties;
        this.connectionMap = new ConcurrentHashMap<>();
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public void open() throws CatalogException {}

    @Override
    public void close() throws CatalogException {
        for (BaseConnection connection : connectionMap.values()) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new CatalogException("Close connection error", e);
            }
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        BaseConnection defaultConnection = getDefaultConnection();
        try (PreparedStatement ps =
                        defaultConnection.prepareStatement("select datname from pg_database;");
                ResultSet rs = ps.executeQuery()) {

            List<String> databases = new ArrayList<>();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!SYS_DATABASES.contains(databaseName)) {
                    databases.add(rs.getString(1));
                }
            }

            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }
        String urlFromDatabaseName = getUrlFromDatabaseName(databaseName);
        BaseConnection connection = getConnection(urlFromDatabaseName);

        try (PreparedStatement ps =
                        connection.prepareStatement(
                                "SELECT table_schema, table_name FROM information_schema.tables;");
                ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String schemaName = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                if (org.apache.commons.lang3.StringUtils.isNotBlank(schemaName)
                        && !SYS_DATABASES.contains(schemaName)) {
                    tables.add(schemaName + "." + tableName);
                }
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (databaseExists(tablePath.getDatabaseName())) {
                String urlFromDatabaseName = getUrlFromDatabaseName(tablePath.getDatabaseName());
                BaseConnection connection = getConnection(urlFromDatabaseName);
                try (PreparedStatement ps =
                        connection.prepareStatement(
                                "SELECT table_schema, table_name FROM information_schema.tables where table_schema = ? and table_name = ?")) {
                    ps.setString(1, tablePath.getSchemaName());
                    ps.setString(2, tablePath.getTableName());
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return false;
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try {
            BaseConnection connection = getConnection(tablePath);
            DatabaseMetaData metaData = connection.getMetaData();
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

            String sql =
                    String.format(
                            SELECT_COLUMNS_SQL,
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            try (PreparedStatement ps = connection.prepareStatement(sql);
                    ResultSet resultSet = ps.executeQuery()) {
                TableSchema.Builder builder = TableSchema.builder();

                // add column
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
                        "",
                        "postgres");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    protected void buildColumnsWithErrorCheck(
            TablePath tablePath, ResultSet resultSet, TableSchema.Builder builder)
            throws SQLException {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (resultSet.next()) {
            try {
                builder.column(buildColumn(resultSet));
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    catalogName, tablePath.getFullName(), unsupported);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        DwsGaussSqlGenerator dwsGaussSqlGenerator =
                new DwsGaussSqlGenerator(
                        readonlyConfig.get(PRIMARY_KEY),
                        readonlyConfig.get(FIELD_IDE),
                        table,
                        readonlyConfig.get(COPY_FILE_FIELD_DELIMITER));
        this.executeUpdateSql(dwsGaussSqlGenerator.getCreateTargetTableSql());
        log.info(
                "Create table: {} success using: {}",
                dwsGaussSqlGenerator.getTargetTableName(),
                dwsGaussSqlGenerator.getCreateTargetTableSql());
        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            this.executeUpdateSql(dwsGaussSqlGenerator.getCreateTemporaryTableSql());
            log.info(
                    "Create temporary table: {} success using: {}",
                    dwsGaussSqlGenerator.getTemporaryTableName(),
                    dwsGaussSqlGenerator.getCreateTemporaryTableSql());
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql = "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + tableName + "\"";
        BaseConnection connection = getConnection(tablePath);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            if (!ps.execute() && !ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
        String sql = "CREATE DATABASE \"" + tablePath.getDatabaseName() + "\"";
        BaseConnection connection = getConnection(tablePath);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (!ps.execute() && !ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            tablePath.getDatabaseName(), this.catalogName),
                    e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String sql = "DROP DATABASE IF EXISTS \"" + tablePath.getDatabaseName() + "\"";
        BaseConnection connection = getConnection(tablePath);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (!ps.execute() && !ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            tablePath.getDatabaseName(), this.catalogName),
                    e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            DwsGaussSqlGenerator dwsGaussSqlGenerator =
                    new DwsGaussSqlGenerator(
                            readonlyConfig.get(PRIMARY_KEY),
                            readonlyConfig.get(FIELD_IDE),
                            catalogTable.get(),
                            readonlyConfig.get(COPY_FILE_FIELD_DELIMITER));
            String sql = dwsGaussSqlGenerator.getCreateTargetTableSql();
            if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                    == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
                sql = sql + "\n" + dwsGaussSqlGenerator.getCreateTemporaryTableSql();
            }
            return new SQLPreviewResult(sql);
        } else if (actionType == ActionType.DROP_TABLE) {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        } else if (actionType == ActionType.CREATE_DATABASE) {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        } else if (actionType == ActionType.DROP_DATABASE) {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }

    public BaseConnection getDefaultConnection() {
        return getConnection(defaultUrl);
    }

    private BaseConnection getConnection(TablePath tablePath) {
        String urlFromDatabaseName = getUrlFromDatabaseName(tablePath.getDatabaseName());
        return getConnection(urlFromDatabaseName);
    }

    private BaseConnection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        Properties properties = new Properties();
        properties.put(USER.key(), username);
        properties.put(PASSWORD.key(), password);
        if (MapUtils.isNotEmpty(jdbcProperties)) {
            properties.putAll(jdbcProperties);
        }
        try {
            BaseConnection baseConnection =
                    (BaseConnection) DriverManager.getConnection(url, properties);
            connectionMap.put(url, baseConnection);
            return baseConnection;
        } catch (SQLException e) {
            throw new CatalogException("Create connection failed", e);
        }
    }

    public int executeUpdateSql(String sql) {
        BaseConnection connection = getDefaultConnection();
        try (Statement preparedStatement = connection.createStatement()) {
            return preparedStatement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Execute Sql: " + sql + " failed", e);
        }
    }

    public void executeSql(String sql) {
        BaseConnection connection = getDefaultConnection();
        try (Statement preparedStatement = connection.createStatement()) {
            preparedStatement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Execute Sql: " + sql + " failed", e);
        }
    }

    public int queryDataCount(String sql) {
        BaseConnection connection = getDefaultConnection();
        try (Statement preparedStatement = connection.createStatement();
                ResultSet resultSet = preparedStatement.executeQuery(sql)) {
            if (resultSet == null) {
                return 0;
            }
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        // seq -> column name
        List<Pair<Integer, String>> primaryKeyColumns = new ArrayList<>();
        String pkName = null;

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        try (ResultSet rs = metaData.getPrimaryKeys(database, schema, table)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                // all the PK_NAME should be the same
                pkName = rs.getString("PK_NAME");
                int keySeq = rs.getInt("KEY_SEQ");
                // KEY_SEQ is 1-based index
                primaryKeyColumns.add(Pair.of(keySeq, columnName));
            }
        }
        // initialize size
        List<String> pkFields =
                primaryKeyColumns.stream()
                        .sorted(Comparator.comparingInt(Pair::getKey))
                        .map(Pair::getValue)
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pkFields)) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(pkName, pkFields));
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String table) throws SQLException {
        return getConstraintKeys(metaData, database, table, table);
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        try (ResultSet resultSet = metaData.getIndexInfo(database, schema, table, false, false)) {
            // index name -> index
            Map<String, ConstraintKey> constraintKeyMap = new HashMap<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                if (columnName == null) {
                    continue;
                }
                String indexName = resultSet.getString("INDEX_NAME");
                boolean noUnique = resultSet.getBoolean("NON_UNIQUE");

                ConstraintKey constraintKey =
                        constraintKeyMap.computeIfAbsent(
                                indexName,
                                s -> {
                                    ConstraintKey.ConstraintType constraintType =
                                            ConstraintKey.ConstraintType.INDEX_KEY;
                                    if (!noUnique) {
                                        constraintType = ConstraintKey.ConstraintType.UNIQUE_KEY;
                                    }
                                    return ConstraintKey.of(
                                            constraintType, indexName, new ArrayList<>());
                                });

                ConstraintKey.ColumnSortType sortType =
                        "A".equals(resultSet.getString("ASC_OR_DESC"))
                                ? ConstraintKey.ColumnSortType.ASC
                                : ConstraintKey.ColumnSortType.DESC;
                ConstraintKey.ConstraintKeyColumn constraintKeyColumn =
                        new ConstraintKey.ConstraintKeyColumn(columnName, sortType);
                constraintKey.getColumnNames().add(constraintKeyColumn);
            }
            return new ArrayList<>(constraintKeyMap.values());
        }
    }

    private Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name");
        String fullTypeName = resultSet.getString("full_type_name");
        long columnLength = resultSet.getLong("column_length");
        int columnScale = resultSet.getInt("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("YES");

        // dealingSpecialNumeric
        if (typeName.equals(DwsGaussDBTypeConverter.PG_NUMERIC) && columnLength < 1) {
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
        return DwsGaussDBTypeConverter.INSTANCE.convert(typeDefine);
    }

    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", DwsGaussDBConfig.CONNECTOR_NAME);
        options.put(URL.key(), baseUrl);
        options.put(TABLE.key(), tablePath.getFullName());
        options.put(DATABASE.key(), tablePath.getDatabaseName());
        options.put(USER.key(), username);
        options.put(PASSWORD.key(), password);
        return options;
    }

    private String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + urlInfo.getSuffix();
    }
}
