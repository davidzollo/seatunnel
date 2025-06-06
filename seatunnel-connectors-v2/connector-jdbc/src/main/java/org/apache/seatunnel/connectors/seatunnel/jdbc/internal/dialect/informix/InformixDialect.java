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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InformixDialect implements JdbcDialect {

    public static final int DEFAULT_INFORMIX_FETCH_SIZE = 128;

    @Override
    public String dialectName() {
        return DatabaseIdentifier.INFORMIX;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new InformixJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new InformixTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean isPrimaryKeyUpdated) {
        return Optional.empty();
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
            statement.setFetchSize(DEFAULT_INFORMIX_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public TablePath parse(String tablePath) {
        String[] paths = tablePath.split(":");
        if (paths.length == 2) {
            String[] schemaAndTable = paths[1].split("\\.");
            return TablePath.of(paths[0], schemaAndTable[0], schemaAndTable[1]);
        }
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (StringUtils.isBlank(tablePath.getDatabaseName())) {
            return tablePath.getSchemaAndTableName();
        }
        return String.format(
                "%s:%s.%s",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return parse(tableName).getSchemaAndTableName();
    }

    @Override
    public Object[] sampleDataFromColumn(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int samplingRate,
            int fetchSize)
            throws Exception {
        if (StringUtils.isBlank(table.getQuery())) {
            String sampleQuery =
                    String.format(
                            "SELECT %s FROM %s WHERE MOD((%s - (SELECT MIN(%s) FROM %s)), %s) = 0 ORDER BY %s",
                            quoteIdentifier(columnName),
                            tableIdentifier(table.getTablePath()),
                            quoteIdentifier(columnName),
                            quoteIdentifier(columnName),
                            tableIdentifier(table.getTablePath()),
                            samplingRate,
                            quoteIdentifier(columnName));
            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
                    List<Object> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(rs.getObject(1));
                    }
                    Object[] resultsArray = results.toArray();
                    Arrays.sort(resultsArray);
                    return resultsArray;
                }
            }
        }

        String sampleQuery =
                String.format(
                        "SELECT %s FROM (%s) AS T", quoteIdentifier(columnName), table.getQuery());
        try (Statement stmt = connection.createStatement()) {
            stmt.setFetchSize(1024);
            try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
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
                                    + "SELECT FIRST %s %s FROM (%s) AS T1 WHERE %s >= ? ORDER BY %s ASC"
                                    + ") AS T2",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT FIRST %s %s FROM %s WHERE %s >= ? ORDER BY %s ASC"
                                    + ") AS T",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            table.getTablePath().getSchemaAndTableName(),
                            quotedColumn,
                            quotedColumn);
        }
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                } else {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
            }
        }
    }

    @Override
    public boolean supportHashSplitter() {
        return false;
    }

    @Override
    public Map<String, String> defaultParameter() {
        Map<String, String> defaultParams = new HashMap<>();
        // Set Informix connection parameters, remove locale settings to let system auto-match
        defaultParams.put("IFX_USEPUT", "1");
        return defaultParams;
    }
}
