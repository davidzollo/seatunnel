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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.sink.SaveModeConstants;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.selectdb.catalog.SelectDBTypeConverter;
import org.apache.seatunnel.connectors.selectdb.util.CreateTableParser;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class SelectDBSaveModeUtil {

    public static String fillingCreateSql(
            String template,
            String database,
            String table,
            TableSchema tableSchema,
            String comment) {
        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            primaryKey =
                    tableSchema.getPrimaryKey().getColumnNames().stream()
                            .map(r -> "`" + r + "`")
                            .collect(Collectors.joining(","));
        }
        String uniqueKey = "";
        if (!tableSchema.getConstraintKeys().isEmpty()) {
            uniqueKey =
                    tableSchema.getConstraintKeys().stream()
                            .flatMap(c -> c.getColumnNames().stream())
                            .map(r -> "`" + r.getColumnName() + "`")
                            .collect(Collectors.joining(","));
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_PRIMARY_KEY),
                        primaryKey);
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_UNIQUE_KEY),
                        uniqueKey);
        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(SelectDBSaveModeUtil::columnToSelectDBType)
                        .collect(Collectors.joining(",\n"));

        if (template.contains(SaveModePlaceHolder.TABLE_NAME.getPlaceHolder())) {
            // TODO: Remove this compatibility config
            template =
                    template.replaceAll(
                            SaveModePlaceHolder.TABLE_NAME.getReplacePlaceHolder(), table);
            log.warn(
                    "The variable placeholder `${table_name}` has been marked as deprecated and will be removed soon, please use `${table}`");
        }

        return template.replaceAll(SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(), database)
                .replaceAll(SaveModePlaceHolder.TABLE.getReplacePlaceHolder(), table)
                .replaceAll(
                        SaveModePlaceHolder.ROWTYPE_FIELDS.getReplacePlaceHolder(), rowTypeFields)
                .replaceAll(
                        SaveModePlaceHolder.COMMENT.getReplacePlaceHolder(),
                        Objects.isNull(comment) ? "" : comment);
    }

    public static String columnToSelectDBType(Column column) {
        checkNotNull(column, "The column is required.");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType = SelectDBTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        return String.format(
                "`%s` %s %s ",
                column.getName(), columnType, column.isNullable() ? "NULL" : "NOT NULL");
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        List<CreateTableParser.ColumnInfo> columnInfosInSeq =
                columnInTemplate.values().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        CreateTableParser.ColumnInfo::getStartIndex))
                        .collect(Collectors.toList());
        for (CreateTableParser.ColumnInfo columnInfo : columnInfosInSeq) {
            String col = columnInfo.getName();
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToSelectDBType(column);
                    String prefix = template.substring(0, columnInfo.getStartIndex() + offset);
                    String suffix = template.substring(offset + columnInfo.getEndIndex());
                    if (prefix.endsWith("`")) {
                        prefix = prefix.substring(0, prefix.length() - 1);
                        offset--;
                    }
                    if (suffix.startsWith("`")) {
                        suffix = suffix.substring(1);
                        offset--;
                    }
                    template = prefix + newCol + suffix;
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }
}
