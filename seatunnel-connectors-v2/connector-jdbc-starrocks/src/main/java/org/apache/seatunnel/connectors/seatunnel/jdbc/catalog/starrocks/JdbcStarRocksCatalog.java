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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.starrocks;

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
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks.JdbcStarRocksTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks.JdbcStarRocksTypeMapper;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class JdbcStarRocksCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s' ORDER BY ORDINAL_POSITION ASC";

    private static final String SELECT_DATABASE_EXISTS =
            "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '%s'";

    private static final String SELECT_TABLE_EXISTS =
            "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

    private StarRocksVersion version;
    private JdbcStarRocksTypeConverter typeConverter;

    public JdbcStarRocksCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo, null);
        this.version = resolveVersion();
        this.typeConverter = new JdbcStarRocksTypeConverter();
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(SELECT_DATABASE_EXISTS, databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                SELECT_TABLE_EXISTS, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SHOW TABLES;";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (version == StarRocksVersion.V_3) {
            return super.getTable(tablePath);
        }
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
            String sql =
                    String.format(
                            "SHOW FULL COLUMNS FROM `%s`.`%s`",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            try (final PreparedStatement ps =
                            conn.prepareStatement(getSelectColumnsSql(tablePath));
                    final ResultSet resultSet = ps.executeQuery();
                    final PreparedStatement preparedStatement = conn.prepareStatement(sql);
                    final ResultSet nullRs = preparedStatement.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, builder, resultSet, nullRs);
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // filter constraint key
                List<String> columnNames = builder.build().getColumnNames();
                constraintKeys =
                        constraintKeys.stream()
                                .filter(
                                        key -> {
                                            boolean valid =
                                                    key.getColumnNames().stream()
                                                            .allMatch(
                                                                    column ->
                                                                            columnNames.contains(
                                                                                    column
                                                                                            .getColumnName()));
                                            if (!valid) {
                                                log.warn(
                                                        "The table {} constraint key [{}] is not supported. {}",
                                                        tablePath,
                                                        key.getConstraintName(),
                                                        key);
                                            }
                                            return valid;
                                        })
                                .collect(Collectors.toList());
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
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
    public CatalogTable getTableIgnoreUnSupportColumn(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (version == StarRocksVersion.V_3) {
            return super.getTableIgnoreUnSupportColumn(tablePath);
        }
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

            String sql =
                    String.format(
                            "SHOW FULL COLUMNS FROM `%s`.`%s`",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            try (final PreparedStatement ps =
                            conn.prepareStatement(getSelectColumnsSql(tablePath));
                    final ResultSet resultSet = ps.executeQuery();
                    final PreparedStatement preparedStatement = conn.prepareStatement(sql);
                    final ResultSet nullRs = preparedStatement.executeQuery()) {

                TableSchema.Builder builder = TableSchema.builder();
                try {
                    buildColumnsWithErrorCheck(tablePath, builder, resultSet, nullRs);
                } catch (SeaTunnelRuntimeException e) {
                    if (e.getSeaTunnelErrorCode() != null
                            && CommonErrorCode.GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR.equals(
                                    e.getSeaTunnelErrorCode())) {
                        log.debug(ExceptionUtils.getMessage(e));
                    } else {
                        throw e;
                    }
                }
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // filter constraint key
                List<String> columnNames = builder.build().getColumnNames();
                constraintKeys =
                        constraintKeys.stream()
                                .filter(
                                        key -> {
                                            boolean valid =
                                                    key.getColumnNames().stream()
                                                            .allMatch(
                                                                    column ->
                                                                            columnNames.contains(
                                                                                    column
                                                                                            .getColumnName()));
                                            if (!valid) {
                                                log.warn(
                                                        "The table {} constraint key [{}] is not supported. {}",
                                                        tablePath,
                                                        key.getConstraintName(),
                                                        key);
                                            }
                                            return valid;
                                        })
                                .collect(Collectors.toList());
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
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

    protected void buildColumnsWithErrorCheck(
            TablePath tablePath, TableSchema.Builder builder, ResultSet... resultSet)
            throws SQLException {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (resultSet[0].next()) {
            try {
                builder.column(buildColumn(resultSet[0], resultSet[1]));
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
    public Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        List<String> primaryKeyList = new ArrayList<>();
        String pkName = null;
        String sql = String.format(SELECT_COLUMNS_SQL_TEMPLATE, database, table);
        try (final PreparedStatement preparedStatement =
                        metaData.getConnection().prepareStatement(sql);
                final ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String keyType = rs.getString("COLUMN_KEY");
                if ("PRI".equals(keyType)) {
                    primaryKeyList.add(columnName);
                }
            }
        }
        if (!primaryKeyList.isEmpty()) {
            return Optional.of(PrimaryKey.of(pkName, primaryKeyList));
        }
        return Optional.empty();
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        List<ConstraintKey> indexList =
                super.getConstraintKeys(
                        metaData,
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName());
        for (Iterator<ConstraintKey> it = indexList.iterator(); it.hasNext(); ) {
            ConstraintKey index = it.next();
            if (ConstraintKey.ConstraintType.UNIQUE_KEY.equals(index.getConstraintType())
                    && "PRIMARY".equals(index.getConstraintName())) {
                it.remove();
            }
        }
        return indexList;
    }

    protected Column buildColumn(ResultSet resultSet, ResultSet NullableResultSet)
            throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        // e.g. tinyint(1) unsigned
        String columnType = resultSet.getString("COLUMN_TYPE");
        // e.g. tinyint
        String dataType = resultSet.getString("DATA_TYPE").toUpperCase();
        String comment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        boolean isNullable = getNullAble(NullableResultSet, columnName);
        // e.g. `decimal(10, 2)` is 10
        long numberPrecision = resultSet.getInt("NUMERIC_PRECISION");
        // e.g. `decimal(10, 2)` is 2
        int numberScale = resultSet.getInt("NUMERIC_SCALE");
        // e.g. `varchar(10)` is 40
        long charOctetLength = resultSet.getLong("CHARACTER_OCTET_LENGTH");
        // e.g. `timestamp(3)` is 3
        int timePrecision = resultSet.getInt("DATETIME_PRECISION");

        Preconditions.checkArgument(!(numberPrecision > 0 && charOctetLength > 0));
        Preconditions.checkArgument(!(numberScale > 0 && timePrecision > 0));

        MysqlType mysqlType = MysqlType.getByName(columnType);
        boolean unsigned = columnType.toLowerCase(Locale.ROOT).contains("unsigned");

        BasicTypeDefine<MysqlType> typeDefine =
                BasicTypeDefine.<MysqlType>builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType)
                        .nativeType(mysqlType)
                        .unsigned(unsigned)
                        .length(Math.max(charOctetLength, numberPrecision))
                        .precision(numberPrecision)
                        .scale(Math.max(numberScale, timePrecision))
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return typeConverter.convert(typeDefine);
    }

    protected Boolean getNullAble(ResultSet rs, String columnName) {
        try {
            while (rs.next()) {
                if (rs.getString("Field").equals(columnName)) {
                    return "Yes".equalsIgnoreCase(rs.getString("Null"));
                }
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", catalogName), e);
        }
        return true;
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return JdbcStarRocksCreateTableSqlBuilder.builder(tablePath, table, typeConverter)
                .build(table.getCatalogName());
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE IF EXISTS `%s`.`%s`;",
                tablePath.getDatabaseName(), tablePath.getTableName());
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
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(
                defaultConnection, sqlQuery, new JdbcStarRocksTypeMapper(typeConverter));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format(
                "TRUNCATE TABLE `%s`.`%s`;", tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT * FROM `%s`.`%s` LIMIT 1;",
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected String getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try (PreparedStatement statement =
                getConnection(defaultUrl)
                        .prepareStatement(
                                "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            statement.setString(1, tablePath.getDatabaseName());
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

    private StarRocksVersion resolveVersion() {
        try (Statement statement = getConnection(defaultUrl).createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT current_version()")) {
            resultSet.next();
            return StarRocksVersion.parse(resultSet.getString(1));
        } catch (Exception e) {
            log.info(
                    "Failed to get starrocks version, fallback to default version: {}",
                    StarRocksVersion.V_2,
                    e);
            return StarRocksVersion.V_2;
        }
    }
}
