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
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashan;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class YashanDialect implements JdbcDialect {

    private static final int DEFAULT_YASHAN_FETCH_SIZE = 256;

    public YashanDialect() {}

    @Override
    public String dialectName() {
        return DatabaseIdentifier.YASHAN;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new YashanJdbcRowConverter();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(HASH(" + quoteIdentifier(fieldName) + ")," + mod + ")";
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new YashanTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            return Arrays.stream(identifier.split("\\."))
                    .map(part -> "\"" + part + "\"")
                    .collect(Collectors.joining("."));
        }
        return "\"" + identifier + "\"";
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(tableName);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean isPrimaryKeyUpdated) {
        String tableIdentifier = tableIdentifier(database, tableName);
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String values =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        String valuesWithAlias =
                Arrays.stream(fieldNames)
                        .map(f -> ":" + f + " AS " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String mergeCondition =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                f ->
                                        "target."
                                                + quoteIdentifier(f)
                                                + " = source."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(" AND "));
        String updateSet =
                Arrays.stream(fieldNames)
                        .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                        .map(f -> quoteIdentifier(f) + " = source." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String upsertSQL =
                String.format(
                        "MERGE INTO %s target "
                                + "USING (SELECT %s FROM DUAL) source "
                                + "ON (%s) "
                                + "WHEN MATCHED THEN UPDATE SET %s "
                                + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                        tableIdentifier,
                        valuesWithAlias,
                        mergeCondition,
                        updateSet,
                        columns,
                        values);

        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_YASHAN_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return quoteIdentifier(tablePath.getSchemaAndTableName());
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        return SQLUtils.countForTable(connection, tableIdentifier(table.getTablePath()));
    }

    public String getExistTableSql(TablePath tablePath) {
        return String.format(
                "SELECT TABLE_NAME FROM DBA_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
                tablePath.getSchemaName().toUpperCase(), tablePath.getTableName().toUpperCase());
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM (%s) WHERE %s >= ? AND ROWNUM <= %s ORDER BY %s ASC"
                                    + ")",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            chunkSize,
                            quotedColumn);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? AND ROWNUM <= %s ORDER BY %s ASC"
                                    + ")",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            chunkSize,
                            quotedColumn);
        }
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
                return rs.getObject(1);
            }
        }
    }

    @Override
    public Object[] sampleDataFromColumn(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int samplingRate,
            int fetchSize)
            throws Exception {
        String sampleQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM (%s) T", quoteIdentifier(columnName), table.getQuery());
        } else {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM %s",
                            quoteIdentifier(columnName), tableIdentifier(table.getTablePath()));
        }
        try (PreparedStatement stmt = creatPreparedStatement(connection, sampleQuery, fetchSize)) {
            try (ResultSet rs = stmt.executeQuery()) {
                int count = 0;
                List<Object> results = new ArrayList<>();
                while (rs.next()) {
                    count++;
                    if (count % samplingRate == 0) {
                        results.add(rs.getObject(1));
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Thread interrupted");
                    }
                }
                Object[] resultsArray = results.toArray();
                Arrays.sort(resultsArray);
                return resultsArray;
            }
        }
    }
}
