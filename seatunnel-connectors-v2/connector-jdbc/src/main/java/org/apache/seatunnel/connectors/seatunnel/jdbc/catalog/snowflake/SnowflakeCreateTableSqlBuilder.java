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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake.SnowflakeTypeConverter;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class SnowflakeCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;

    public SnowflakeCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
    }

    public String build(String tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE IF NOT EXISTS ").append(tablePath).append(" (\n");

        List<String> columnSqls =
                columns.stream().map(this::buildColumnSql).collect(Collectors.toList());

        // Add primary key constraint if it has multiple columns
        if (primaryKey != null && primaryKey.getColumnNames().size() > 1) {
            columnSqls.add(
                    "PRIMARY KEY ("
                            + primaryKey.getColumnNames().stream()
                                    .map(column -> "\"" + column + "\"")
                                    .collect(Collectors.joining(", "))
                            + ")");
        }

        createTableSql.append(String.join(",\n    ", columnSqls));
        createTableSql.append("\n)");

        return createTableSql.toString();
    }

    String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        // Get column type
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (sourceCatalogName.equalsIgnoreCase(DatabaseIdentifier.SNOWFLAKE)
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = SnowflakeTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a single-column primary key
        if (primaryKey != null
                && primaryKey.getColumnNames().contains(column.getName())
                && primaryKey.getColumnNames().size() == 1) {
            columnSql.append(" PRIMARY KEY");
        }

        // Add column comment if present
        if (StringUtils.isNotBlank(column.getComment())) {
            columnSql
                    .append(" COMMENT '")
                    .append(column.getComment().replace("'", "''"))
                    .append("'");
        }

        return columnSql.toString();
    }
}
