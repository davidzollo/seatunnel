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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class PostgresCreateTableSqlBuilder extends AbstractJdbcCreateTableSqlBuilder {
    private static final List<String> COMPATIBLE_DATABASES =
            Arrays.asList(
                    DatabaseIdentifier.POSTGRESQL.toUpperCase(),
                    DatabaseIdentifier.HIGHGO.toUpperCase());

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String comment;
    private String sourceCatalogName;
    private String fieldIde;
    private List<ConstraintKey> constraintKeys;
    public Boolean isHaveConstraintKey = false;
    private Collection<String> pgPlugins;

    @Getter public List<String> createIndexSqls = new ArrayList<>();

    public PostgresCreateTableSqlBuilder(CatalogTable catalogTable) {
        this(catalogTable, Collections.emptyList());
    }

    public PostgresCreateTableSqlBuilder(CatalogTable catalogTable, Collection<String> pgPlugins) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.comment = catalogTable.getComment();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
        this.pgPlugins = pgPlugins;
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE IF NOT EXISTS ", fieldIde))
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }

        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && StringUtils.equals(
                                        primaryKey.getPrimaryKey(),
                                        constraintKey.getConstraintName()))) {
                    continue;
                }
                switch (constraintKey.getConstraintType()) {
                    case UNIQUE_KEY:
                        isHaveConstraintKey = true;
                        Map<String, ConstraintKey.ConstraintKeyColumn> uniqueKeyColumns =
                                constraintKey.getColumnNames().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName,
                                                        e -> e,
                                                        (a, b) -> a,
                                                        LinkedHashMap::new));
                        if (primaryKey != null
                                && primaryKey.getColumnNames() != null
                                && !primaryKey.getColumnNames().isEmpty()) {
                            for (String pkColumn : primaryKey.getColumnNames()) {
                                if (!uniqueKeyColumns.containsKey(pkColumn)) {
                                    uniqueKeyColumns.put(
                                            pkColumn,
                                            new ConstraintKey.ConstraintKeyColumn(
                                                    pkColumn, ConstraintKey.ColumnSortType.ASC));
                                }
                            }
                            if (uniqueKeyColumns.size() != constraintKey.getColumnNames().size()) {
                                log.info(
                                        "Primary key columns are added to unique key columns, new unique key columns: {}",
                                        uniqueKeyColumns.values());
                            }
                        }

                        constraintKey =
                                ConstraintKey.of(
                                        constraintKey.getConstraintType(),
                                        constraintKey.getConstraintName(),
                                        new ArrayList<>(uniqueKeyColumns.values()));

                        String uniqueKeySql = buildUniqueKeySql(constraintKey);
                        columnSqls.add("\t" + uniqueKeySql);
                        break;
                    case INDEX_KEY:
                        isHaveConstraintKey = true;
                        String indexKeySql = buildIndexKeySql(tablePath, constraintKey);
                        createIndexSqls.add(indexKeySql);
                        break;
                    case FOREIGN_KEY:
                        // todo: add foreign key
                        break;
                }
            }
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");
        if (comment != null) {
            createTableSql.append("\n");
            createTableSql.append("COMMENT ON TABLE ");
            createTableSql.append(tablePath.getSchemaAndTableName("\""));
            createTableSql.append(" IS '").append(comment).append("';");
        }

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns, tablePath.getSchemaAndTableName("\"")))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append("\n");
            createTableSql.append(String.join(";\n", commentSqls)).append(";");
        }

        return createTableSql.toString();
    }

    private String buildPrimaryKeySql() {
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
    }

    String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in PostgreSQL
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (isCompatibleCatalog(sourceCatalogName)
                && StringUtils.isNotBlank(column.getSourceType())) {
            if ((column.getSourceType().startsWith(PostgresTypeConverter.PG_POSTGIS_GEOMETRY)
                            || column.getSourceType()
                                    .startsWith(PostgresTypeConverter.PG_POSTGIS_GEOGRAPHY))
                    && !pgPlugins.contains(PostgresTypeConverter.PG_POSTGIS)) {
                columnType = buildColumnType(column);
            } else {
                columnType = column.getSourceType();
            }
        } else {
            columnType = buildColumnType(column);
        }
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        return columnSql.toString();
    }

    protected boolean isCompatibleCatalog(String sourceCatalogName) {
        return COMPATIBLE_DATABASES.contains(sourceCatalogName.toUpperCase());
    }

    protected String buildColumnType(Column column) {
        return PostgresTypeConverter.INSTANCE.reconvert(column).getColumnType();
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier("COMMENT ON COLUMN ", fieldIde))
                .append(tableName)
                .append(".");
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "\""))
                .append(CatalogUtils.quoteIdentifier(" IS '", fieldIde))
                .append(column.getComment().replace("'", "''").replace("\\", "\\\\"))
                .append("'");
        return columnCommentSql.toString();
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String constraintName = UUID.randomUUID().toString().replace("-", "");
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));
        return "CONSTRAINT \"" + constraintName + "\" UNIQUE (" + indexColumns + ")";
    }

    private String buildIndexKeySql(TablePath tablePath, ConstraintKey constraintKey) {
        // If the index name is omitted, PostgreSQL will choose an appropriate name based on table
        // name and indexed columns.
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));

        return "CREATE INDEX ON "
                + tablePath.getSchemaAndTableName("\"")
                + "("
                + indexColumns
                + ");";
    }
}
