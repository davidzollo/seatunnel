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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
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
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.client.SnowflakeCopyIntoClient;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.util.SnowflakeIdentifierUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class SnowflakeFileCatalog implements Catalog {

    private final SnowflakeFileConfig config;
    private SnowflakeCopyIntoClient client;

    @Override
    public String name() {
        return "SnowflakeFile";
    }

    public SnowflakeFileCatalog(SnowflakeFileConfig config) {
        this.config = config;
    }

    @Override
    public void open() throws CatalogException {
        try {
            this.client = new SnowflakeCopyIntoClient(config);
            this.client.connect();
            log.info("SnowflakeFileCatalog opened successfully");
        } catch (SQLException e) {
            throw new CatalogException("Failed to open SnowflakeFileCatalog", e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (client != null) {
            try {
                client.close();
                log.info("SnowflakeFileCatalog closed successfully");
            } catch (SQLException e) {
                throw new CatalogException("Failed to close SnowflakeFileCatalog", e);
            }
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return config.getDatabase();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            // Use SHOW DATABASES to check database existence
            String sql = "SHOW DATABASES";
            log.info(
                    "Checking database existence with SQL: {} and filtering for: {}",
                    sql,
                    databaseName);

            try (Statement stmt = client.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String dbName = rs.getString("name");
                    if (databaseName.equalsIgnoreCase(dbName)) {
                        log.info("Database {} found", databaseName);
                        return true;
                    }
                }
            }
            log.info("Database {} not found", databaseName);
            return false;
        } catch (SQLException e) {
            log.error("Failed to check database existence for {}", databaseName, e);
            throw new CatalogException("Failed to check database existence", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            List<String> databases = new ArrayList<>();
            String sql = "SHOW DATABASES";
            try (Statement stmt = client.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    databases.add(rs.getString("name"));
                }
            }
            return databases;
        } catch (SQLException e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException {
        try {
            List<String> tables = new ArrayList<>();
            // Use INFORMATION_SCHEMA to list tables
            String sql =
                    String.format(
                            "SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
                            databaseName);
            log.info("Listing tables with SQL: {}", sql);
            try (Statement stmt = client.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            log.info("Found {} tables in database {}", tables.size(), databaseName);
            return tables;
        } catch (SQLException e) {
            log.error("Failed to list tables in database {}", databaseName, e);
            throw new CatalogException("Failed to list tables", e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            // Use INFORMATION_SCHEMA to check table existence
            String sql =
                    String.format(
                            "SELECT COUNT(*) FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                            SnowflakeIdentifierUtils.quoteIdentifier(tablePath.getDatabaseName()),
                            escapeSqlLiteral(tablePath.getSchemaName()),
                            escapeSqlLiteral(tablePath.getTableName()));
            log.info("Checking table existence with SQL: {}", sql);

            // Execute count query
            int count = client.executeCountQuery(sql);
            boolean exists = count > 0;

            log.info(
                    "Table {}.{}.{} exists: {}",
                    tablePath.getDatabaseName(),
                    tablePath.getSchemaName(),
                    tablePath.getTableName(),
                    exists);
            return exists;
        } catch (SQLException e) {
            log.error(
                    "Failed to check table existence for {}.{}.{}",
                    tablePath.getDatabaseName(),
                    tablePath.getSchemaName(),
                    tablePath.getTableName(),
                    e);
            throw new CatalogException("Failed to check table existence", e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(name(), tablePath);
        }

        try {
            DatabaseMetaData metaData = client.getConnection().getMetaData();
            TableSchema.Builder builder = TableSchema.builder();
            try (ResultSet resultSet =
                    metaData.getColumns(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            null)) {
                while (resultSet.next()) {
                    builder.column(buildColumn(resultSet));
                }
            }

            getPrimaryKey(metaData, tablePath).ifPresent(builder::primaryKey);

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            name(),
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    getTableComment(metaData, tablePath),
                    name());
        } catch (SQLException e) {
            throw new CatalogException(
                    "Failed to get table " + tablePath.getFullName() + " from Snowflake", e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable catalogTable, boolean ignoreIfExists)
            throws CatalogException, TableAlreadyExistException, DatabaseNotExistException {
        try {
            // Build CREATE TABLE statement
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ");
            if (ignoreIfExists) {
                sql.append("IF NOT EXISTS ");
            }
            sql.append(quoteTablePath(tablePath)).append(" (");

            // Add column definitions
            List<Column> columns = catalogTable.getTableSchema().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                Column column = columns.get(i);
                sql.append(quoteColumnName(column.getName()))
                        .append(" ")
                        .append(convertToSnowflakeType(column));
            }

            sql.append(")");

            log.info("Creating table with SQL: {}", sql.toString());
            client.executeSql(sql.toString());
            log.info("Table {} created successfully", tablePath.getFullName());

        } catch (SQLException e) {
            throw new CatalogException("Failed to create table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws CatalogException, TableNotExistException {
        try {
            String sql =
                    ignoreIfNotExists
                            ? String.format("DROP TABLE IF EXISTS %s", quoteTablePath(tablePath))
                            : String.format("DROP TABLE %s", quoteTablePath(tablePath));
            log.info("Dropping table with SQL: {}", sql);
            client.executeSql(sql);
            log.info("Table {} dropped successfully", tablePath.getFullName());
        } catch (SQLException e) {
            log.error("Failed to drop table {}", tablePath.getFullName(), e);
            throw new CatalogException("Failed to drop table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void createDatabase(TablePath databasePath, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistException {
        try {
            String sql =
                    ignoreIfExists
                            ? "CREATE DATABASE IF NOT EXISTS "
                                    + SnowflakeIdentifierUtils.quoteIdentifier(
                                            databasePath.getDatabaseName())
                            : "CREATE DATABASE "
                                    + SnowflakeIdentifierUtils.quoteIdentifier(
                                            databasePath.getDatabaseName());
            log.info("Creating database with SQL: {}", sql);
            client.executeSql(sql);
            log.info("Database {} created successfully", databasePath.getDatabaseName());
        } catch (SQLException e) {
            log.error("Failed to create database {}", databasePath.getDatabaseName(), e);
            throw new CatalogException(
                    "Failed to create database " + databasePath.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(TablePath databasePath, boolean cascade)
            throws CatalogException, DatabaseNotExistException {
        try {
            String sql =
                    "DROP DATABASE "
                            + SnowflakeIdentifierUtils.quoteIdentifier(
                                    databasePath.getDatabaseName());
            if (cascade) {
                sql += " CASCADE";
            }
            log.info("Dropping database with SQL: {}", sql);
            client.executeSql(sql);
            log.info("Database {} dropped successfully", databasePath.getDatabaseName());
        } catch (SQLException e) {
            log.error("Failed to drop database {}", databasePath.getDatabaseName(), e);
            throw new CatalogException(
                    "Failed to drop database " + databasePath.getDatabaseName(), e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws CatalogException, TableNotExistException {
        try {
            if (ignoreIfNotExists && !tableExists(tablePath)) {
                return;
            }
            String sql = String.format("TRUNCATE TABLE %s", quoteTablePath(tablePath));
            client.executeSql(sql);
            log.info("Table {} truncated successfully", tablePath.getFullName());
        } catch (SQLException e) {
            throw new CatalogException("Failed to truncate table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) throws CatalogException {
        try {
            String sql = String.format("SELECT COUNT(*) FROM %s", quoteTablePath(tablePath));
            return client.executeCountQuery(sql) > 0;
        } catch (SQLException e) {
            throw new CatalogException(
                    "Failed to check if data exists in table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) throws CatalogException {
        try {
            client.executeSql(sql);
            log.info("SQL executed successfully: {}", sql);
        } catch (SQLException e) {
            throw new CatalogException("Failed to execute SQL: " + sql, e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable)
            throws CatalogException {
        String sql = null;
        switch (actionType) {
            case CREATE_TABLE:
                if (catalogTable.isPresent()) {
                    sql = buildCreateTableSql(tablePath, catalogTable.get());
                }
                break;
            case DROP_TABLE:
                sql = String.format("DROP TABLE %s", quoteTablePath(tablePath));
                break;
            case TRUNCATE_TABLE:
                sql = String.format("TRUNCATE TABLE %s", quoteTablePath(tablePath));
                break;
            case CREATE_DATABASE:
                sql =
                        String.format(
                                "CREATE DATABASE %s",
                                SnowflakeIdentifierUtils.quoteIdentifier(
                                        tablePath.getDatabaseName()));
                break;
            case DROP_DATABASE:
                sql =
                        String.format(
                                "DROP DATABASE %s",
                                SnowflakeIdentifierUtils.quoteIdentifier(
                                        tablePath.getDatabaseName()));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
        return new SQLPreviewResult(sql);
    }

    private String buildCreateTableSql(TablePath tablePath, CatalogTable catalogTable) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteTablePath(tablePath)).append(" (");

        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            Column column = columns.get(i);
            sql.append(quoteColumnName(column.getName()))
                    .append(" ")
                    .append(convertToSnowflakeType(column));
        }

        sql.append(")");
        return sql.toString();
    }

    private String quoteTablePath(TablePath tablePath) {
        return SnowflakeIdentifierUtils.quoteQualifiedIdentifier(
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    private String quoteColumnName(String columnName) {
        return SnowflakeIdentifierUtils.quoteIdentifier(columnName);
    }

    private String escapeSqlLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private String convertToSnowflakeType(Column column) {
        // Use SnowflakeTypeConverter for type conversion
        return SnowflakeTypeConverter.INSTANCE.reconvert(column).getColumnType();
    }

    private Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = normalizeTypeName(resultSet.getString("TYPE_NAME"));
        long rawColumnLength = resultSet.getLong("COLUMN_SIZE");
        boolean columnLengthWasNull = resultSet.wasNull();
        int rawColumnScale = resultSet.getInt("DECIMAL_DIGITS");
        boolean columnScaleWasNull = resultSet.wasNull();
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean nullable = resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
        Long columnLength = columnLengthWasNull ? null : rawColumnLength;
        Integer columnScale = columnScaleWasNull ? null : rawColumnScale;

        return SnowflakeTypeConverter.INSTANCE.convert(
                org.apache.seatunnel.api.table.converter.BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(nullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build());
    }

    private Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        List<String> columnNames = new ArrayList<>();
        String primaryKeyName = null;
        try (ResultSet resultSet =
                metaData.getPrimaryKeys(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName())) {
            while (resultSet.next()) {
                columnNames.add(resultSet.getString("COLUMN_NAME"));
                if (primaryKeyName == null) {
                    primaryKeyName = resultSet.getString("PK_NAME");
                }
            }
        }
        if (columnNames.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(primaryKeyName, columnNames));
    }

    private String getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try (ResultSet resultSet =
                metaData.getTables(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        null)) {
            while (resultSet.next()) {
                return resultSet.getString("REMARKS");
            }
        }
        return "";
    }

    private String normalizeTypeName(String rawTypeName) {
        if (rawTypeName == null) {
            return null;
        }
        switch (rawTypeName.toUpperCase(java.util.Locale.ROOT)) {
            case "TIMESTAMPNTZ":
                return "TIMESTAMP_NTZ";
            case "TIMESTAMPLTZ":
                return "TIMESTAMP_LTZ";
            case "TIMESTAMPTZ":
                return "TIMESTAMP_TZ";
            default:
                return rawTypeName;
        }
    }
}
