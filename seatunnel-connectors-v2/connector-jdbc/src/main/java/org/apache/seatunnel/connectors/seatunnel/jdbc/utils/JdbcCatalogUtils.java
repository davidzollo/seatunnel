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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class JdbcCatalogUtils {
    private static final String DEFAULT_CATALOG_NAME = "jdbc_catalog";

    public static Map<TablePath, JdbcSourceTable> getTables(
            JdbcConnectionConfig jdbcConnectionConfig, List<JdbcSourceTableConfig> tablesConfig)
            throws SQLException, ClassNotFoundException {
        Map<TablePath, JdbcSourceTable> tables = new LinkedHashMap<>();

        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcConnectionConfig.getUrl(),
                        jdbcConnectionConfig.getCompatibleMode(),
                        jdbcConnectionConfig.getDialect());
        Optional<Catalog> catalog = findCatalog(jdbcConnectionConfig, jdbcDialect);
        if (catalog.isPresent()) {
            try (AbstractJdbcCatalog jdbcCatalog = (AbstractJdbcCatalog) catalog.get()) {
                log.info("Loading catalog tables for catalog : {}", jdbcCatalog.getClass());

                jdbcCatalog.open();
                Map<String, Map<String, String>> unsupportedTable = new LinkedHashMap<>();
                for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                    try {
                        CatalogTable catalogTable =
                                getCatalogTable(tableConfig, jdbcCatalog, jdbcDialect);
                        TablePath tablePath = catalogTable.getTableId().toTablePath();
                        String sourceQuery = getNormalizedSourceQuery(tableConfig, jdbcDialect);
                        JdbcSourceTable jdbcSourceTable =
                                JdbcSourceTable.builder()
                                        .tablePath(tablePath)
                                        .query(sourceQuery)
                                        .jdbcUrl(
                                                resolveTableJdbcUrl(
                                                        jdbcConnectionConfig,
                                                        jdbcCatalog,
                                                        tablePath))
                                        .partitionColumn(tableConfig.getPartitionColumn())
                                        .partitionNumber(tableConfig.getPartitionNumber())
                                        .partitionStart(tableConfig.getPartitionStart())
                                        .partitionEnd(tableConfig.getPartitionEnd())
                                        .useSelectCount(tableConfig.getUseSelectCount())
                                        .skipAnalyze(tableConfig.getSkipAnalyze())
                                        .catalogTable(catalogTable)
                                        .build();
                        tables.put(tablePath, jdbcSourceTable);
                        if (log.isDebugEnabled()) {
                            log.debug("Loaded catalog table : {}, {}", tablePath, jdbcSourceTable);
                        }
                    } catch (SeaTunnelRuntimeException e) {
                        if (e.getSeaTunnelErrorCode()
                                .equals(
                                        CommonErrorCode
                                                .GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR)) {
                            unsupportedTable.put(
                                    e.getParams().get("tableName"),
                                    e.getParamsValueAsMap("fieldWithDataTypes"));
                        } else {
                            throw e;
                        }
                    }
                }
                if (!unsupportedTable.isEmpty()) {
                    throw CommonError.getCatalogTablesWithUnsupportedType(
                            jdbcDialect.dialectName(), unsupportedTable);
                }
                log.info(
                        "Loaded {} catalog tables for catalog : {}",
                        tables.size(),
                        jdbcCatalog.getClass());
            }
            return tables;
        }

        log.warn(
                "Catalog not found, loading tables from jdbc directly. url : {}",
                jdbcConnectionConfig.getUrl());
        try (Connection connection = getConnection(jdbcConnectionConfig, jdbcDialect)) {
            log.info("Loading catalog tables for jdbc : {}", jdbcConnectionConfig.getUrl());
            for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                CatalogTable catalogTable = getCatalogTable(tableConfig, connection, jdbcDialect);
                TablePath tablePath = catalogTable.getTableId().toTablePath();
                String sourceQuery = getNormalizedSourceQuery(tableConfig, jdbcDialect);
                JdbcSourceTable jdbcSourceTable =
                        JdbcSourceTable.builder()
                                .tablePath(tablePath)
                                .query(sourceQuery)
                                .jdbcUrl(jdbcConnectionConfig.getUrl())
                                .partitionColumn(tableConfig.getPartitionColumn())
                                .partitionNumber(tableConfig.getPartitionNumber())
                                .partitionStart(tableConfig.getPartitionStart())
                                .partitionEnd(tableConfig.getPartitionEnd())
                                .useSelectCount(tableConfig.getUseSelectCount())
                                .skipAnalyze(tableConfig.getSkipAnalyze())
                                .catalogTable(catalogTable)
                                .build();

                tables.put(tablePath, jdbcSourceTable);
                if (log.isDebugEnabled()) {
                    log.debug("Loaded catalog table : {}, {}", tablePath, jdbcSourceTable);
                }
            }
            log.info(
                    "Loaded {} catalog tables for jdbc : {}",
                    tables.size(),
                    jdbcConnectionConfig.getUrl());
            return tables;
        }
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig,
            AbstractJdbcCatalog jdbcCatalog,
            JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath = jdbcCatalog.getTableIgnoreUnSupportColumn(tablePath);
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            String sqlQuery =
                    normalizePostgresFamilyTablePathQuery(
                            tableConfig.getQuery(), tablePath, jdbcDialect);
            CatalogTable tableOfQuery = jdbcCatalog.getTable(tablePath, sqlQuery);
            if (tableOfPath == null) {
                String catalogName =
                        tableOfQuery.getTableId() == null
                                ? DEFAULT_CATALOG_NAME
                                : tableOfQuery.getTableId().getCatalogName();
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            return jdbcCatalog.getTable(tablePath);
        }

        return jdbcCatalog.getTable(tableConfig.getQuery());
    }

    /**
     * Keep runtime split/read connections on the same database that the catalog used for this
     * table_path.
     */
    static String resolveTableJdbcUrl(
            JdbcConnectionConfig jdbcConnectionConfig,
            AbstractJdbcCatalog jdbcCatalog,
            TablePath tablePath) {
        if (jdbcCatalog == null || tablePath == null) {
            return jdbcConnectionConfig.getUrl();
        }
        return jdbcCatalog.getTableJdbcUrl(tablePath);
    }

    static CatalogTable mergeCatalogTable(CatalogTable tableOfPath, CatalogTable tableOfQuery) {
        TableSchema tableSchemaOfPath = tableOfPath.getTableSchema();
        Map<String, Column> columnsOfPath =
                tableSchemaOfPath.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        Function.identity(),
                                        (o1, o2) -> o1,
                                        LinkedHashMap::new));
        TableSchema tableSchemaOfQuery = tableOfQuery.getTableSchema();
        Map<String, Column> columnsOfQuery =
                tableSchemaOfQuery.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        Function.identity(),
                                        (o1, o2) -> o1,
                                        LinkedHashMap::new));
        Set<String> columnKeysOfQuery = columnsOfQuery.keySet();

        List<Column> columnsOfMerge =
                tableSchemaOfQuery.getColumns().stream()
                        .filter(
                                column ->
                                        columnsOfPath.containsKey(column.getName())
                                                && columnsOfPath
                                                        .get(column.getName())
                                                        .getDataType()
                                                        .getSqlType()
                                                        .equals(
                                                                columnsOfQuery
                                                                        .get(column.getName())
                                                                        .getDataType()
                                                                        .getSqlType()))
                        .map(column -> columnsOfPath.get(column.getName()))
                        .collect(Collectors.toList());
        boolean schemaIncludeAllColumns = columnsOfMerge.size() == columnKeysOfQuery.size();
        boolean schemaEquals =
                schemaIncludeAllColumns && columnsOfMerge.size() == columnsOfPath.size();
        if (schemaEquals) {
            // Reorder the field list
            return CatalogTable.of(
                    tableOfPath.getTableId(),
                    TableSchema.builder()
                            .primaryKey(tableSchemaOfPath.getPrimaryKey())
                            .constraintKey(tableSchemaOfPath.getConstraintKeys())
                            .columns(columnsOfMerge)
                            .build(),
                    tableOfPath.getOptions(),
                    tableOfPath.getPartitionKeys(),
                    tableOfPath.getComment());
        }

        PrimaryKey primaryKeyOfPath = tableSchemaOfPath.getPrimaryKey();
        List<ConstraintKey> constraintKeysOfPath = tableSchemaOfPath.getConstraintKeys();
        List<String> partitionKeysOfPath = tableOfPath.getPartitionKeys();
        PrimaryKey primaryKeyOfMerge = null;
        List<ConstraintKey> constraintKeysOfMerge = new ArrayList<>();
        List<String> partitionKeysOfMerge = new ArrayList<>();

        if (primaryKeyOfPath != null
                && columnKeysOfQuery.containsAll(primaryKeyOfPath.getColumnNames())) {
            primaryKeyOfMerge = primaryKeyOfPath;
        }
        if (constraintKeysOfPath != null) {
            for (ConstraintKey constraintKey : constraintKeysOfPath) {
                Set<String> constraintKeyFields =
                        constraintKey.getColumnNames().stream()
                                .map(e -> e.getColumnName())
                                .collect(Collectors.toSet());
                if (columnKeysOfQuery.containsAll(constraintKeyFields)) {
                    constraintKeysOfMerge.add(constraintKey);
                }
            }
        }
        if (partitionKeysOfPath != null && columnKeysOfQuery.containsAll(partitionKeysOfPath)) {
            partitionKeysOfMerge = partitionKeysOfPath;
        }
        if (schemaIncludeAllColumns) {
            return CatalogTable.of(
                    tableOfPath.getTableId(),
                    TableSchema.builder()
                            .primaryKey(primaryKeyOfMerge)
                            .constraintKey(constraintKeysOfMerge)
                            .columns(columnsOfMerge)
                            .build(),
                    tableOfPath.getOptions(),
                    partitionKeysOfMerge,
                    tableOfPath.getComment());
        }

        String catalogName =
                tableOfQuery.getTableId() == null
                        ? DEFAULT_CATALOG_NAME
                        : tableOfQuery.getTableId().getCatalogName();
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogName,
                        tableOfPath.getTableId().getDatabaseName(),
                        tableOfPath.getTableId().getSchemaName(),
                        tableOfPath.getTableId().getTableName());
        CatalogTable mergedCatalogTable =
                CatalogTable.of(
                        tableIdentifier,
                        TableSchema.builder()
                                .primaryKey(primaryKeyOfMerge)
                                .constraintKey(constraintKeysOfMerge)
                                .columns(tableSchemaOfQuery.getColumns())
                                .build(),
                        tableOfPath.getOptions(),
                        partitionKeysOfMerge,
                        tableOfPath.getComment());

        log.info("Merged catalog table of path {}", tableOfPath.getTableId().toTablePath());
        return mergedCatalogTable;
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig, Connection connection, JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath =
                        CatalogUtils.getCatalogTable(
                                connection, tablePath, jdbcDialect.getJdbcDialectTypeMapper());
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            String sqlQuery =
                    normalizePostgresFamilyTablePathQuery(
                            tableConfig.getQuery(), tablePath, jdbcDialect);
            CatalogTable tableOfQuery = getCatalogTable(connection, sqlQuery, jdbcDialect);
            if (tableOfPath == null) {
                String catalogName =
                        tableOfQuery.getTableId() == null
                                ? DEFAULT_CATALOG_NAME
                                : tableOfQuery.getTableId().getCatalogName();
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            return CatalogUtils.getCatalogTable(
                    connection, tablePath, jdbcDialect.getJdbcDialectTypeMapper());
        }

        return getCatalogTable(connection, tableConfig.getQuery(), jdbcDialect);
    }

    private static CatalogTable getCatalogTable(
            Connection connection, String sqlQuery, JdbcDialect jdbcDialect) throws SQLException {
        ResultSetMetaData resultSetMetaData =
                jdbcDialect.getResultSetMetaData(connection, sqlQuery);
        return CatalogUtils.getCatalogTable(
                resultSetMetaData, jdbcDialect.getJdbcDialectTypeMapper(), sqlQuery);
    }

    /**
     * Returns the SQL query that should be stored in JdbcSourceTable after metadata loading.
     *
     * <p>The normalized query is intentionally limited to table_path + query cases. Query-only
     * configurations do not have a physical table path that can safely define the database context.
     */
    private static String getNormalizedSourceQuery(
            JdbcSourceTableConfig tableConfig, JdbcDialect jdbcDialect) {
        if (StringUtils.isEmpty(tableConfig.getTablePath())
                || StringUtils.isEmpty(tableConfig.getQuery())) {
            return tableConfig.getQuery();
        }
        TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
        return normalizePostgresFamilyTablePathQuery(
                tableConfig.getQuery(), tablePath, jdbcDialect);
    }

    /**
     * Removes the database qualifier emitted by older Web versions for PostgreSQL-family generated
     * projection SQL.
     *
     * <p>PostgreSQL-compatible databases do not support cross-database relation names inside a
     * database connection. When table_path already selects the target database, a legacy query like
     * {@code FROM "db"."schema"."table"} must be prepared and executed as {@code FROM
     * "schema"."table"} to stay compatible with origin/2.6-release Web output.
     */
    static String normalizePostgresFamilyTablePathQuery(
            String sqlQuery, TablePath tablePath, JdbcDialect jdbcDialect) {
        if (StringUtils.isBlank(sqlQuery)
                || tablePath == null
                || !isPostgresFamilyDialect(jdbcDialect)
                || StringUtils.isBlank(tablePath.getDatabaseName())
                || StringUtils.isBlank(tablePath.getSchemaName())
                || StringUtils.isBlank(tablePath.getTableName())) {
            return sqlQuery;
        }

        String database = tablePath.getDatabaseName();
        String schema = tablePath.getSchemaName();
        String table = tablePath.getTableName();
        String normalizedQuery =
                replaceRelationQualifier(
                        sqlQuery,
                        quotePgIdentifier(database)
                                + "."
                                + quotePgIdentifier(schema)
                                + "."
                                + quotePgIdentifier(table),
                        quotePgIdentifier(schema) + "." + quotePgIdentifier(table));
        return replaceRelationQualifier(
                normalizedQuery, database + "." + schema + "." + table, schema + "." + table);
    }

    /**
     * Checks whether a dialect uses PostgreSQL-style database and schema semantics for relation
     * names.
     */
    private static boolean isPostgresFamilyDialect(JdbcDialect jdbcDialect) {
        if (jdbcDialect == null) {
            return false;
        }
        String dialectName = jdbcDialect.dialectName();
        return DatabaseIdentifier.POSTGRESQL.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.OPENGAUSS.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.HIGHGO.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.GAUSSDB.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.GREENPLUM.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.REDSHIFT.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.HOLOGRES.equalsIgnoreCase(dialectName)
                || DatabaseIdentifier.ANALYTIC_DB_PG.equalsIgnoreCase(dialectName);
    }

    /**
     * Replaces a matching FROM/JOIN relation only when the relation exactly matches table_path.
     *
     * <p>This keeps custom predicates and other SQL clauses untouched.
     */
    private static String replaceRelationQualifier(
            String sqlQuery, String relation, String replacementRelation) {
        Pattern pattern =
                Pattern.compile(
                        "\\b(FROM|JOIN)\\s+" + Pattern.quote(relation) + "(?=\\s|$|;|\\)|,)",
                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sqlQuery);
        return matcher.replaceAll("$1 " + Matcher.quoteReplacement(replacementRelation));
    }

    /**
     * Quotes a PostgreSQL identifier and escapes embedded double quotes.
     *
     * <p>The generated value is only used for exact relation matching.
     */
    private static String quotePgIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static Connection getConnection(JdbcConnectionConfig config, JdbcDialect jdbcDialect)
            throws SQLException, ClassNotFoundException {
        JdbcConnectionProvider connectionProvider = jdbcDialect.getJdbcConnectionProvider(config);
        return connectionProvider.getOrEstablishConnection();
    }

    public static Optional<Catalog> findCatalog(JdbcConnectionConfig config, JdbcDialect dialect) {
        ReadonlyConfig catalogConfig = extractCatalogConfig(config);
        return FactoryUtil.createOptionalCatalog(
                dialect.dialectName(),
                catalogConfig,
                JdbcCatalogUtils.class.getClassLoader(),
                dialect.dialectName());
    }

    private static ReadonlyConfig extractCatalogConfig(JdbcConnectionConfig config) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put(JdbcCatalogOptions.BASE_URL.key(), config.getUrl());
        config.getUsername()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.USERNAME.key(), val));
        config.getPassword()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.PASSWORD.key(), val));
        Optional.ofNullable(config.getCompatibleMode())
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.COMPATIBLE_MODE.key(), val));
        catalogConfig.put(
                JdbcOptions.DECIMAL_TYPE_NARROWING.key(), config.isDecimalTypeNarrowing());
        catalogConfig.put(JdbcOptions.INT_TYPE_NARROWING.key(), config.isIntTypeNarrowing());
        catalogConfig.put(JdbcOptions.BIT_TYPE_NARROWING.key(), config.isBitTypeNarrowing());
        return ReadonlyConfig.fromMap(catalogConfig);
    }
}
