/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.yashan;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashan.YashanTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashan.YashanTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class YashanCatalog extends AbstractJdbcCatalog {

    private static final String TABLE_NAME_REGEX =
            "(?i)\\b(?:FROM|JOIN)\\s+([a-zA-Z0-9_$.]+)(?:\\s+AS\\s+[a-zA-Z0-9_$]+)?";

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    c.COLUMN_NAME,\n"
                    + "    c.DATA_TYPE AS TYPE_NAME,\n"
                    + "    CASE\n"
                    + "        WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL\n"
                    + "            THEN c.DATA_TYPE || '(' || c.DATA_PRECISION || ',' || c.DATA_SCALE || ')'\n"
                    + "        WHEN c.DATA_TYPE IN ('VARCHAR2', 'CHAR')\n"
                    + "            THEN c.DATA_TYPE || '(' || c.CHAR_LENGTH || ')'\n"
                    + "        ELSE c.DATA_TYPE\n"
                    + "    END AS FULL_TYPE_NAME,\n"
                    + "    c.CHAR_LENGTH AS COLUMN_LENGTH,\n"
                    + "    c.DATA_PRECISION AS COLUMN_PRECISION,\n"
                    + "    c.DATA_SCALE AS COLUMN_SCALE,\n"
                    + "    cm.COMMENTS AS COLUMN_COMMENT,\n"
                    + "    c.DATA_DEFAULT AS DEFAULT_VALUE,\n"
                    + "    c.NULLABLE AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    ALL_TAB_COLUMNS c\n"
                    + "LEFT JOIN\n"
                    + "    ALL_COL_COMMENTS cm\n"
                    + "    ON c.OWNER = cm.OWNER\n"
                    + "    AND c.TABLE_NAME = cm.TABLE_NAME\n"
                    + "    AND c.COLUMN_NAME = cm.COLUMN_NAME\n"
                    + "WHERE\n"
                    + "    c.OWNER = '%s'\n"
                    + "    AND c.TABLE_NAME = '%s'"
                    + "ORDER BY c.COLUMN_ID";

    private boolean decimalTypeNarrowing;

    public YashanCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        this(
                catalogName,
                username,
                pwd,
                urlInfo,
                defaultSchema,
                JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue());
    }

    public YashanCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            boolean decimalTypeNarrowing) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.decimalTypeNarrowing = decimalTypeNarrowing;
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return getListTableSql(tablePath.getDatabaseName())
                + "  and  OWNER = '"
                + tablePath.getSchemaName()
                + "' AND TABLE_NAME = '"
                + tablePath.getTableName()
                + "'";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl =
                StringUtils.isNotBlank(tablePath.getDatabaseName())
                        ? getUrlFromDatabaseName(tablePath.getDatabaseName())
                        : getUrlFromDatabaseName(defaultDatabase);

        try (Connection conn = getConnection(dbUrl)) {
            Optional<PrimaryKey> primaryKey = getPrimaryKey(conn, tablePath);

            List<ConstraintKey> constraintKeys = getConstraintKeys(conn, tablePath);
            constraintKeys = filterDuplicateConstraintKeys(constraintKeys, primaryKey);

            String tableComment = getTableComment(null, tablePath);

            try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
                    ResultSet resultSet = ps.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);

                primaryKey.ifPresent(builder::primaryKey);
                constraintKeys.forEach(builder::constraintKey);

                return CatalogTable.of(
                        getTableIdentifier(tablePath),
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        tableComment,
                        catalogName);
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to get table %s", tablePath.getFullName()), e);
        }
    }

    private Optional<PrimaryKey> getPrimaryKey(Connection conn, TablePath tablePath)
            throws SQLException {
        String sql =
                String.format(
                        "SELECT cols.COLUMN_NAME\n"
                                + "FROM ALL_CONSTRAINTS cons\n"
                                + "JOIN ALL_CONS_COLUMNS cols\n"
                                + "ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME\n"
                                + "WHERE cons.OWNER = '%s'\n"
                                + "  AND cons.TABLE_NAME = '%s'\n"
                                + "  AND cons.CONSTRAINT_TYPE = 'P'\n"
                                + "ORDER BY cols.POSITION",
                        tablePath.getSchemaName(), tablePath.getTableName());

        List<String> pkColumns = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }

        if (pkColumns.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(PrimaryKey.of("PK_" + tablePath.getTableName(), pkColumns));
    }

    private List<ConstraintKey> getConstraintKeys(Connection conn, TablePath tablePath)
            throws SQLException {
        String sql =
                String.format(
                        "SELECT idx.INDEX_NAME, idx.INDEX_TYPE, cols.COLUMN_NAME, cols.COLUMN_POSITION\n"
                                + "FROM ALL_INDEXES idx\n"
                                + "JOIN ALL_IND_COLUMNS cols\n"
                                + "ON idx.INDEX_NAME = cols.INDEX_NAME\n"
                                + "WHERE idx.TABLE_OWNER = '%s'\n"
                                + "  AND idx.TABLE_NAME = '%s'",
                        tablePath.getSchemaName(), tablePath.getTableName());

        Map<String, ConstraintKey> constraintMap = new LinkedHashMap<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String indexType = rs.getString("INDEX_TYPE");
                String columnName = rs.getString("COLUMN_NAME");
                int position = rs.getInt("COLUMN_POSITION");

                if (indexName.startsWith("PK_")) continue;

                ConstraintKey.ConstraintType type = mapConstraintType(indexType);

                constraintMap
                        .computeIfAbsent(
                                indexName,
                                k ->
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.valueOf(type.name()),
                                                indexName,
                                                new ArrayList<>()))
                        .getColumnNames()
                        .add(
                                ConstraintKey.ConstraintKeyColumn.of(
                                        columnName, ConstraintKey.ColumnSortType.ASC));
            }
        }

        return new ArrayList<>(constraintMap.values());
    }

    private ConstraintKey.ConstraintType mapConstraintType(String indexType) {
        switch (indexType.toUpperCase()) {
            case "UNIQUE":
                return ConstraintKey.ConstraintType.UNIQUE_KEY;
            case "FOREIGN":
                return ConstraintKey.ConstraintType.FOREIGN_KEY;
            default:
                return ConstraintKey.ConstraintType.INDEX_KEY;
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>(Collections.singletonList("default"));
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new YashanCreateTableSqlBuilder(table).build(tablePath).get(0);
    }

    protected List<String> getCreateTableSqls(TablePath tablePath, CatalogTable table) {
        return new YashanCreateTableSqlBuilder(table).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getTableName());
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES where DATABASE_MAINTAINED = 'N'";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        Long columnPrecision = resultSet.getObject("COLUMN_PRECISION", Long.class);
        Integer columnScale = resultSet.getObject("COLUMN_SCALE", Integer.class);
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("Y");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return new YashanTypeConverter(decimalTypeNarrowing).convert(typeDefine);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        String schemaName = username.toUpperCase();
        String tableName = "";
        Pattern pattern = Pattern.compile(TABLE_NAME_REGEX);
        Matcher matcher = pattern.matcher(sqlQuery);

        if (matcher.find()) {
            String fullTableName = matcher.group(1);
            String[] parts = fullTableName.split("\\.");

            if (parts.length == 2) {
                schemaName = parts[0];
                tableName = parts[1];
            } else {
                tableName = fullTableName;
            }
        }
        if (StringUtils.isBlank(tableName)) {
            throw new SQLException("Table name not found in the SQL query.");
        }
        return getCatalogTable(
                defaultConnection,
                schemaName,
                tableName,
                new YashanTypeMapper(decimalTypeNarrowing));
    }

    public static CatalogTable getCatalogTable(
            Connection connection,
            String schemaName,
            String tableName,
            JdbcDialectTypeMapper typeMapper)
            throws SQLException {
        String columnQuery = String.format(SELECT_COLUMNS_SQL_TEMPLATE, schemaName, tableName);

        List<Column> columnList = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(columnQuery)) {
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String typeName = resultSet.getString("TYPE_NAME");
                String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
                long columnLength = resultSet.getLong("COLUMN_LENGTH");
                Long columnPrecision = resultSet.getObject("COLUMN_PRECISION", Long.class);
                Integer columnScale = resultSet.getObject("COLUMN_SCALE", Integer.class);
                String columnComment = resultSet.getString("COLUMN_COMMENT");
                Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
                boolean isNullable = resultSet.getString("IS_NULLABLE").equals("Y");

                BasicTypeDefine typeDefine =
                        BasicTypeDefine.builder()
                                .name(columnName)
                                .columnType(fullTypeName)
                                .dataType(typeName)
                                .length(columnLength)
                                .precision(columnPrecision)
                                .scale(columnScale)
                                .nullable(isNullable)
                                .defaultValue(defaultValue)
                                .comment(columnComment)
                                .build();

                SeaTunnelDataType<?> seatunnelType =
                        typeMapper.mappingColumn(typeDefine).getDataType();
                Column column =
                        PhysicalColumn.of(
                                columnName,
                                seatunnelType,
                                columnLength,
                                isNullable,
                                defaultValue,
                                columnComment,
                                fullTypeName,
                                null);
                columnList.add(column);
            }
        }

        TableSchema tableSchema = TableSchema.builder().columns(columnList).build();

        return CatalogTable.of(
                TableIdentifier.of("catalog_name", schemaName, tableName),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "Table comment");
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format("TRUNCATE TABLE %s", tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format("SELECT * FROM %s LIMIT 1", tablePath.getTableName());
    }

    @Override
    protected String getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try (PreparedStatement statement =
                getConnection(defaultUrl)
                        .prepareStatement(
                                "SELECT COMMENTS FROM DBA_TAB_COMMENTS WHERE OWNER = ? AND TABLE_NAME = ?")) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                return null;
            }
        } catch (SQLException e) {
            log.warn("Failed to get table comment", e);
            return null;
        }
    }
}
