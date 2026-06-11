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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog.ClickhouseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.common.util.CatalogUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class ClickhouseCatalogUtil extends CatalogUtil {

    public static final ClickhouseCatalogUtil INSTANCE = new ClickhouseCatalogUtil();

    public String columnToConnectorType(Column column) {
        checkNotNull(column, "The column is required.");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType = ClickhouseTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }

        // Add Nullable() wrapper if column is nullable
        if (column.isNullable()) {
            columnType = "Nullable(" + columnType + ")";
        }

        return String.format(
                "`%s` %s %s",
                column.getName(),
                columnType,
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '"
                                + column.getComment().replace("'", "''").replace("\\", "\\\\")
                                + "'");
    }

    public String getCreateTableSql(
            String template,
            String database,
            String table,
            TableSchema tableSchema,
            String comment,
            Map<String, String> options,
            String optionsKey) {
        String createTableSql =
                super.getCreateTableSql(
                        template, database, table, tableSchema, comment, optionsKey);
        String indexDefinitions = buildIndexDefinitions(tableSchema, options);
        if (StringUtils.isEmpty(indexDefinitions)) {
            return createTableSql;
        }
        return injectIndexes(createTableSql, indexDefinitions);
    }

    private String buildIndexDefinitions(TableSchema tableSchema, Map<String, String> options) {
        if (tableSchema == null
                || tableSchema.getConstraintKeys() == null
                || tableSchema.getConstraintKeys().isEmpty()) {
            return "";
        }
        if (options == null || options.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        for (ConstraintKey constraintKey : constraintKeys) {
            if (constraintKey == null
                    || constraintKey.getConstraintType()
                            != ConstraintKey.ConstraintType.INDEX_KEY) {
                continue;
            }
            String indexName = constraintKey.getConstraintName();
            if (StringUtils.isBlank(indexName)) {
                continue;
            }
            String baseKey = ClickhouseSinkOptions.INDEX_OPTION_PREFIX + indexName;
            String expr = options.get(baseKey + ClickhouseSinkOptions.INDEX_OPTION_EXPR_SUFFIX);
            if (StringUtils.isBlank(expr)) {
                continue;
            }
            String typeFull =
                    options.get(baseKey + ClickhouseSinkOptions.INDEX_OPTION_TYPE_FULL_SUFFIX);
            String granularity =
                    options.get(baseKey + ClickhouseSinkOptions.INDEX_OPTION_GRANULARITY_SUFFIX);

            if (sb.length() > 0) {
                sb.append(",\n");
            }
            sb.append("    INDEX ").append(indexName).append(" ").append(expr);
            if (StringUtils.isNotBlank(typeFull)) {
                sb.append(" TYPE ").append(typeFull);
            }
            if (StringUtils.isNotBlank(granularity)) {
                sb.append(" GRANULARITY ").append(granularity);
            }
        }
        return sb.toString();
    }

    private String injectIndexes(String createTableSql, String indexDefinitions) {
        if (StringUtils.isEmpty(indexDefinitions)) {
            return createTableSql;
        }

        int engineIndex = createTableSql.indexOf("ENGINE");
        if (engineIndex < 0) {
            return createTableSql;
        }
        int closeParenIndex = createTableSql.lastIndexOf(")", engineIndex);
        if (closeParenIndex < 0) {
            return createTableSql;
        }
        int columnsEndNewline = createTableSql.lastIndexOf('\n', closeParenIndex - 1);
        if (columnsEndNewline < 0) {
            String before = createTableSql.substring(0, closeParenIndex);
            String after = createTableSql.substring(closeParenIndex);
            return before + ",\n" + indexDefinitions + after;
        }
        String before = createTableSql.substring(0, columnsEndNewline);
        String after = createTableSql.substring(columnsEndNewline);
        return before + ",\n" + indexDefinitions + after;
    }

    public String getDropTableSql(TablePath tablePath, boolean ignoreIfNotExists) {
        if (ignoreIfNotExists) {
            return "DROP TABLE IF EXISTS " + getQuotedTableFullName(tablePath);
        } else {
            return "DROP TABLE " + getQuotedTableFullName(tablePath);
        }
    }

    public String getTruncateTableSql(TablePath tablePath) {
        return "TRUNCATE TABLE " + getQuotedTableFullName(tablePath);
    }

    private String getQuotedTableFullName(TablePath tablePath) {
        return ClickhouseUtil.quoteTableIdentifier(
                tablePath.getDatabaseName() + "." + tablePath.getTableName());
    }
}
