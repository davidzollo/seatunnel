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

package org.apache.seatunnel.transform.fieldreplacer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformExceptionUtil;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldReplacerTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "FieldReplacer";

    // 这个只是任务初始化的时候使用，运行过程中发生DDL inputCatalogTable 不会改变
    private List<CatalogTable> inputCatalogTable;
    // 这个里面保存的是字段名变更的历史，可以通过启动时的字段名找到这个字段最新的字段名
    private Map<String, TableHistory> tableHistoryMap;
    // 这个里面的定义的类型和上游过来的数据一致
    private Map<String, SeaTunnelRowType> seaTunnelRowTypeMap;
    // 这个里面保存的是最新针对每个字段的操作
    private Map<String, Map<String, FieldReplacerTransformConfig.FieldReplacer>> fieldBaseConfMap;
    private FieldReplacerTransformConfig config;

    public FieldReplacerTransform(
            List<CatalogTable> inputCatalogTable, FieldReplacerTransformConfig config) {
        this.inputCatalogTable = inputCatalogTable;
        this.tableHistoryMap =
                inputCatalogTable.stream()
                        .map(TableHistory::build)
                        .collect(
                                Collectors.toMap(
                                        TableHistory::getCurrentTableName, v -> v, (v1, v2) -> v1));
        this.seaTunnelRowTypeMap =
                inputCatalogTable.stream()
                        .collect(
                                Collectors.toMap(
                                        c -> c.getTableId().toTablePath().getFullName(),
                                        v -> v.getTableSchema().toPhysicalRowDataType(),
                                        (v1, v2) -> v1));
        this.config = config;
        reFresh();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return inputCatalogTable;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        String tableId = row.getTableId();
        Map<String, FieldReplacerTransformConfig.FieldReplacer> stringFieldBaseConfMap =
                fieldBaseConfMap.get(tableId);
        SeaTunnelRowType seaTunnelRowType = seaTunnelRowTypeMap.get(tableId);
        if (stringFieldBaseConfMap == null) {
            return row;
        }
        stringFieldBaseConfMap.forEach(
                (filedName, conf) -> {
                    int pos = seaTunnelRowType.indexOf(filedName);
                    Object value = row.getField(pos);
                    row.setField(pos, replacerValue(value, conf));
                });
        return row;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return inputCatalogTable.get(0);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return getProducedCatalogTable().getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        String fullName = schemaChangeEvent.tablePath().getFullName();
        TableHistory tableHistory = tableHistoryMap.get(fullName);
        if (tableHistory != null)
            tableHistoryMap
                    .get(schemaChangeEvent.tablePath().getFullName())
                    .applySchemaChangeEvent(schemaChangeEvent);
        SeaTunnelRowType seaTunnelRowType = seaTunnelRowTypeMap.get(fullName);
        final DataTypeChangeEventDispatcher dataTypeChangeEventDispatcher =
                new DataTypeChangeEventDispatcher();
        dataTypeChangeEventDispatcher.reset(seaTunnelRowType);
        seaTunnelRowTypeMap.put(fullName, dataTypeChangeEventDispatcher.handle(schemaChangeEvent));
        reFresh();
        return schemaChangeEvent;
    }

    private Object replacerValue(
            Object inputFieldValue, FieldReplacerTransformConfig.FieldReplacer config) {
        // 将空值替换为具体的值
        if (config.getReplaceNull() != null && config.getReplaceNull()) {
            if (null == inputFieldValue) {
                return config.getReplacements().values().stream().findFirst().get();
            }
            return inputFieldValue;
        }

        // 空值不做任何处理
        if (null == inputFieldValue) {
            return inputFieldValue;
        }

        if (config.getReplaceToNull() != null && config.getReplaceToNull()) {
            return null;
        }

        boolean isRegex = config.getIsRegex() != null && config.getIsRegex();
        for (String replacementKey : config.getReversedReplacementsKey()) {
            String replacedValue = inputFieldValue.toString();
            if (isRegex) {
                if (config.getReplaceFirst()) {
                    replacedValue =
                            inputFieldValue
                                    .toString()
                                    .replaceFirst(
                                            replacementKey,
                                            config.getReplacements().get(replacementKey));
                } else {
                    replacedValue =
                            inputFieldValue
                                    .toString()
                                    .replaceAll(
                                            replacementKey,
                                            config.getReplacements().get(replacementKey));
                }
            } else {
                replacedValue =
                        inputFieldValue
                                .toString()
                                .replace(
                                        replacementKey,
                                        config.getReplacements().get(replacementKey));
            }
            if (!replacedValue.equals(inputFieldValue)) {
                return replacedValue;
            }
        }
        return inputFieldValue;
    }

    private void reFresh() {
        Map<String, Map<String, FieldReplacerTransformConfig.FieldReplacer>> newFieldBaseConfMap =
                new HashMap<>();
        TransformExceptionUtil.withErrorCheck(
                PLUGIN_NAME,
                config.getFieldReplacers().iterator(),
                replacer -> {
                    if (!tableHistoryMap.containsKey(replacer.getTablePath())) {
                        throw TransformCommonError.cannotFindInputTableError(
                                PLUGIN_NAME, replacer.getTablePath());
                    }
                    String tableName =
                            tableHistoryMap.get(replacer.getTablePath()).getCurrentTableName();
                    Map<String, FieldReplacerTransformConfig.FieldReplacer> columnRules;
                    if (newFieldBaseConfMap.containsKey(tableName)) {
                        columnRules = newFieldBaseConfMap.get(tableName);
                    } else {
                        columnRules = new HashMap<>();
                        newFieldBaseConfMap.put(tableName, columnRules);
                    }
                    columnRules.put(
                            tableHistoryMap
                                    .get(replacer.getTablePath())
                                    .getCurrentColumnName(replacer.getReplaceField()),
                            replacer);
                });
        this.fieldBaseConfMap = newFieldBaseConfMap;
        checkConf();
    }

    private void checkConf() {
        // 校验fieldBaseConfMap 中配置的列是否在 seaTunnelRowTypeMap 中存在
        fieldBaseConfMap.forEach(
                (tableName, filedMap) -> {
                    if (seaTunnelRowTypeMap.containsKey(tableName)) {
                        SeaTunnelRowType seaTunnelRowType = seaTunnelRowTypeMap.get(tableName);
                        filedMap.keySet()
                                .forEach(
                                        filedName -> {
                                            // 保证filedName在seaTunnelRowType 中存在
                                            if (!Arrays.asList(seaTunnelRowType.getFieldNames())
                                                    .contains(filedName)) {
                                                throw new SeaTunnelException(
                                                        String.format(
                                                                "[%s] 中配置的列 [%s] 在输入表结构 [%s] 中不存在",
                                                                tableName,
                                                                filedName,
                                                                Arrays.toString(
                                                                        seaTunnelRowType
                                                                                .getFieldNames())));
                                            }
                                        });
                    } else {
                        throw new SeaTunnelException(
                                String.format(
                                        "配置的表[%s],在输入的表中[%s]不存在",
                                        tableName, seaTunnelRowTypeMap.keySet()));
                    }
                });
    }

    @AllArgsConstructor
    public static class TableHistory implements Serializable {

        LinkedList<String> tablePathHistory;

        Map<String, LinkedList<String>> tableColumnHistory;

        // 处理列改名和表改名
        public void applySchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
            // 处理列改名和表改名
            if (schemaChangeEvent instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                        (AlterTableChangeColumnEvent) schemaChangeEvent;
                String newColumnName = alterTableChangeColumnEvent.getColumn().getName();
                String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
                tableColumnHistory
                        .values()
                        .forEach(
                                columnHistory -> {
                                    if (columnHistory.getLast().equals(oldColumnName)) {
                                        columnHistory.addLast(newColumnName);
                                    }
                                });
                return;
            }
            // 处理表改名
            if (schemaChangeEvent instanceof AlterTableNameEvent) {
                AlterTableNameEvent alterTableNameEvent = (AlterTableNameEvent) schemaChangeEvent;
                String newTableName = alterTableNameEvent.getNewTablePath().getFullName();
                String oldTableName = alterTableNameEvent.getTablePath().getFullName();
                if (tablePathHistory.getLast().equals(oldTableName)) {
                    tablePathHistory.addLast(newTableName);
                }
                return;
            }
            if (schemaChangeEvent instanceof AlterTableColumnsEvent) {
                AlterTableColumnsEvent alterTableColumnsEvent =
                        (AlterTableColumnsEvent) schemaChangeEvent;
                alterTableColumnsEvent.getEvents().forEach(this::applySchemaChangeEvent);
            }
        }

        public String getCurrentTableName() {
            return tablePathHistory.getLast();
        }

        public String getCurrentColumnName(String columnName) {
            if (!tableColumnHistory.containsKey(columnName)) {
                throw TransformCommonError.cannotFindInputTableFieldError(
                        PLUGIN_NAME, getCurrentTableName(), columnName);
            }
            return tableColumnHistory.get(columnName).getLast();
        }

        public static TableHistory build(CatalogTable catalogTable) {
            LinkedList<String> tablePathHistory = new LinkedList<>();

            Map<String, LinkedList<String>> tableColumnHistory = new HashMap<>();

            tablePathHistory.add(catalogTable.getTableId().toTablePath().getFullName());
            catalogTable
                    .getTableSchema()
                    .getColumns()
                    .forEach(
                            column -> {
                                String name = column.getName();
                                tableColumnHistory.put(
                                        name, new LinkedList<>(Collections.singletonList(name)));
                            });
            return new TableHistory(tablePathHistory, tableColumnHistory);
        }
    }
}
