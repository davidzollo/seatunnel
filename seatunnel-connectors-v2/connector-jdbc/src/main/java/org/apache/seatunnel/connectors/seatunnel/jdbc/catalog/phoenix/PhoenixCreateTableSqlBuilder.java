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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.phoenix;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.phoenix.PhoenixTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PhoenixCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;
    private String fieldIde;
    private List<ConstraintKey> constraintKeys;
    public Boolean isHaveConstraintKey = false;

    @Getter public List<String> createIndexSqls = new ArrayList<>();

    public PhoenixCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE ", fieldIde))
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (primaryKey != null && primaryKey.getColumnNames().size() >= 1) {
            columnSqls.add(
                    "CONSTRAINT "
                            + CatalogUtils.quoteIdentifier(primaryKey.getPrimaryKey(), fieldIde)
                            + "_1"
                            + " PRIMARY KEY ("
                            + primaryKey.getColumnNames().stream()
                                    .map(
                                            column ->
                                                    CatalogUtils.quoteIdentifier(
                                                            column, fieldIde, "\""))
                                    .collect(Collectors.joining(", "))
                            + ")");
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
                isHaveConstraintKey = true;
                switch (constraintKey.getConstraintType()) {
                    case UNIQUE_KEY:
                        String uniqueKeySql = buildUniqueKeySql(constraintKey);
                        columnSqls.add("\t" + uniqueKeySql);
                        break;
                    case INDEX_KEY:
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
        createTableSql.append("\n)");
        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in phoenix
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (StringUtils.equalsIgnoreCase(DatabaseIdentifier.PHOENIX, sourceCatalogName)
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = buildColumnType(column);
        }
        columnSql.append(columnType);
        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        return PhoenixTypeConverter.INSTANCE.reconvert(column).getColumnType();
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String constraintName = constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
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
        return "CONSTRAINT " + constraintName + " UNIQUE (" + indexColumns + ")";
    }

    private String buildIndexKeySql(TablePath tablePath, ConstraintKey constraintKey) {
        // We add table name to index name to avoid name conflict in phoenix
        // Since index name in phoenix should unique in the schema
        String constraintName = tablePath.getTableName() + "_" + constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
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

        return "CREATE INDEX "
                + constraintName
                + " ON "
                + tablePath.getSchemaAndTableName("\"")
                + "("
                + indexColumns
                + ");";
    }
}
