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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix.InformixTypeConverter;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class InformixCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;

    public InformixCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
    }

    public String build(TablePath tablePath) {
        return build(tablePath, "");
    }

    public String build(TablePath tablePath, String fieldIde) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE ", fieldIde))
                .append(tablePath.getSchemaAndTableName())
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (primaryKey != null && primaryKey.getColumnNames().size() > 1) {
            columnSqls.add(
                    CatalogUtils.quoteIdentifier(
                            "PRIMARY KEY ("
                                    + primaryKey.getColumnNames().stream()
                                            .map(
                                                    column ->
                                                            CatalogUtils.quoteIdentifier(
                                                                    column, fieldIde))
                                            .collect(Collectors.joining(", "))
                                    + ")",
                            fieldIde));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns,
                                                tablePath.getSchemaAndTableName(),
                                                fieldIde))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append("\n");
            createTableSql.append(String.join(";\n", commentSqls)).append(";");
        }

        return createTableSql.toString();
    }

    String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append(column.getName()).append(" ");

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in Informix
        // SQL
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (sourceCatalogName.equalsIgnoreCase(DatabaseIdentifier.INFORMIX)
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = InformixTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (primaryKey != null
                && primaryKey.getColumnNames().contains(column.getName())
                && primaryKey.getColumnNames().size() == 1) {
            columnSql.append(" PRIMARY KEY");
        }

        // Add default value if exists
        //        if (column.getDefaultValue() != null) {
        //            columnSql.append(" DEFAULT
        // '").append(column.getDefaultValue().toString()).append("'");
        //        }

        return columnSql.toString();
    }

    private String buildColumnCommentSql(Column column, String tableName, String fieldIde) {
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
}
