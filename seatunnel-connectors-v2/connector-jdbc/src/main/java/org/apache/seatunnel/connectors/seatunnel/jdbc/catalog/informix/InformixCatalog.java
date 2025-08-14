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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.informix;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix.InformixTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix.InformixTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class InformixCatalog extends AbstractJdbcCatalog {

    public InformixCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select FIRST 1 * from %s:%s.%s",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "select TRIM(name) from sysmaster:sysdatabases";
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps =
                        connection.prepareStatement(
                                "SELECT owner, tabname FROM "
                                        + databaseName
                                        + ":informix.systables");
                ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String schemaName = rs.getString("owner").trim();
                String tableName = rs.getString("tabname").trim();
                if (org.apache.commons.lang3.StringUtils.isNotBlank(schemaName)) {
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
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return getTableInternal(tablePath, null, false);
    }

    @Override
    public CatalogTable getTableIgnoreUnSupportColumn(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return getTableInternal(tablePath, null, true);
    }

    @Override
    public CatalogTable getTable(TablePath tablePath, List<String> fieldNames)
            throws CatalogException, TableNotExistException {
        return getTableInternal(tablePath, fieldNames, false);
    }

    private CatalogTable getTableInternal(
            TablePath tablePath, List<String> fieldNames, boolean ignoreUnsupported)
            throws CatalogException, TableNotExistException {
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
            constraintKeys = filterDuplicateConstraintKeys(constraintKeys, primaryKey);
            String tableComment = getTableComment(metaData, tablePath);
            try (ResultSet rs =
                    metaData.getColumns(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            null)) {
                TableSchema.Builder builder = TableSchema.builder();
                Map<String, String> unsupported = new LinkedHashMap<>();
                while (rs.next()) {
                    try {
                        Column col = buildColumn(rs);
                        String name = col.getName();
                        if (fieldNames == null || fieldNames.contains(name)) {
                            builder.column(col);
                        }
                    } catch (SeaTunnelRuntimeException e) {
                        boolean isConvertError =
                                CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE.equals(
                                        e.getSeaTunnelErrorCode());
                        if (isConvertError && !ignoreUnsupported) {
                            String fld = e.getParams().get("field");
                            if (fieldNames == null || fieldNames.contains(fld)) {
                                unsupported.put(fld, e.getParams().get("dataType"));
                            }
                        } else if (!isConvertError || !ignoreUnsupported) {
                            throw e;
                        }
                    }
                }

                if (!ignoreUnsupported && !unsupported.isEmpty()) {
                    throw CommonError.getCatalogTableWithUnsupportedType(
                            catalogName, tablePath.getFullName(), unsupported);
                }
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
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_SIZE");
        int columnScale = resultSet.getInt("DECIMAL_DIGITS");
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = resultSet.getInt("NULLABLE") == 1;

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return InformixTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new InformixCreateTableSqlBuilder(table)
                .build(tablePath, table.getOptions().get("fieldIde"));
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();
        return "DROP TABLE " + schemaName + "." + tableName;
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "truncate table %s:%s.%s",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " WHERE TRIM(name) = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "SELECT owner, tabname FROM %s:informix.systables WHERE owner = '%s' AND tabname = '%s'",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
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
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(defaultUrl), sqlQuery, new InformixTypeMapper());
    }
}
