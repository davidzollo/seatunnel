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

package org.apache.seatunnel.transform.tablerenamer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableRenamerTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "TableRenamer";
    private List<CatalogTable> inputCatalogTable;
    private final TableRenamerConfig config;
    private Map<String, String> tableNameMapping = new HashMap<>();
    private Map<String, String> tableIdMapping = new HashMap<>();
    private Map<String, String> specificMap;

    public TableRenamerTransform(List<CatalogTable> inputCatalogTable, TableRenamerConfig config) {
        this.inputCatalogTable =
                inputCatalogTable.stream().map(CatalogTable::copy).collect(Collectors.toList());
        this.config = config;
        this.specificMap =
                Optional.ofNullable(config.getSpecific())
                        .map(
                                e ->
                                        e.stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                TableRenamerConfig.SpecificModify
                                                                        ::getTableName,
                                                                TableRenamerConfig.SpecificModify
                                                                        ::getTargetName)))
                        .orElse(Collections.emptyMap());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return getProducedCatalogTables().get(0);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return getProducedCatalogTable().getSeaTunnelRowType();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        preCheckForConfig(inputCatalogTable);

        List<CatalogTable> outputCatalogTable = new ArrayList<>();
        for (CatalogTable table : inputCatalogTable) {
            String oldTableName = table.getTablePath().getTableName();
            String newTableName = convertName(oldTableName);
            TablePath newTablePath =
                    TablePath.of(
                            table.getTableId().getDatabaseName(),
                            table.getTableId().getSchemaName(),
                            newTableName);
            CatalogTable newCatalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(table.getTableId().getCatalogName(), newTablePath),
                            table);
            outputCatalogTable.add(newCatalogTable);
            tableNameMapping.put(oldTableName, newTableName);
            tableIdMapping.put(table.getTablePath().getFullName(), newTablePath.getFullName());
        }

        return outputCatalogTable;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        if (row.getTableId() == null) {
            return row;
        }
        String tableId = tableIdMapping.get(row.getTableId());
        if (tableId == null || tableId.equals(row.getTableId())) {
            return row;
        }

        SeaTunnelRow newRow = row.copy();
        newRow.setTableId(tableId);
        return newRow;
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        TablePath tablePath = event.tablePath();
        if (tablePath == null) {
            return event;
        }
        String oldTableName = tablePath.getTableName();
        String newTableName = tableNameMapping.get(oldTableName);
        if (newTableName == null || newTableName.equals(oldTableName)) {
            return event;
        }

        if (event instanceof AlterTableColumnsEvent) {
            TableIdentifier newTableIdentifier =
                    TableIdentifier.of(
                            event.tableIdentifier().getCatalogName(),
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            newTableName);
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            return new AlterTableColumnsEvent(
                    newTableIdentifier,
                    alterTableColumnsEvent.getEvents().stream()
                            .map(this::convertName)
                            .collect(Collectors.toList()));
        }
        if (event instanceof AlterTableColumnEvent) {
            return convertName((AlterTableColumnEvent) event);
        }
        return event;
    }

    @VisibleForTesting
    public AlterTableColumnEvent convertName(AlterTableColumnEvent event) {
        TablePath tablePath = event.tablePath();
        if (tablePath == null) {
            return event;
        }
        String oldTableName = tablePath.getTableName();
        String newTableName = tableNameMapping.get(oldTableName);
        if (newTableName == null || newTableName.equals(oldTableName)) {
            return event;
        }

        TableIdentifier newTableIdentifier =
                TableIdentifier.of(
                        event.tableIdentifier().getCatalogName(),
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        newTableName);
        AlterTableColumnEvent newEvent = event;
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                newEvent =
                        new AlterTableAddColumnEvent(
                                newTableIdentifier,
                                addColumnEvent.getColumn(),
                                addColumnEvent.isFirst(),
                                addColumnEvent.getAfterColumn());
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                newEvent =
                        new AlterTableDropColumnEvent(
                                newTableIdentifier, dropColumnEvent.getColumn());
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
                newEvent =
                        new AlterTableModifyColumnEvent(
                                newTableIdentifier,
                                modifyColumnEvent.getColumn(),
                                modifyColumnEvent.isFirst(),
                                modifyColumnEvent.getAfterColumn());
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                newEvent =
                        new AlterTableChangeColumnEvent(
                                newTableIdentifier,
                                changeColumnEvent.getOldColumn(),
                                changeColumnEvent.getColumn(),
                                changeColumnEvent.isFirst(),
                                changeColumnEvent.getAfterColumn());
                break;
            default:
                break;
        }

        newEvent.setJobId(event.getJobId());
        newEvent.setStatement(event.getStatement());
        return newEvent;
    }

    @VisibleForTesting
    public String convertName(String tableName) {
        Optional<String> specificValue = getSpecificModify(tableName);
        if (specificValue.isPresent()) {
            return specificValue.get();
        }

        String replaceTo = null;
        Map<Integer, Integer> replaceIndex = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(config.getReplacementsWithRegex())) {
            for (TableRenamerConfig.ReplacementsWithRegex replacementsWithRegex :
                    config.getReplacementsWithRegex()) {
                Boolean isRegex = replacementsWithRegex.getIsRegex();
                String replacement = replacementsWithRegex.getReplaceFrom();
                if (StringUtils.isNotEmpty(replacement)) {
                    Map<Integer, Integer> matched = new LinkedHashMap<>();
                    if (BooleanUtils.isNotTrue(isRegex)) {
                        if (StringUtils.equals(replacement, tableName)) {
                            matched.put(0, tableName.length());
                        }
                    } else {
                        Matcher matcher = Pattern.compile(replacement).matcher(tableName);
                        while (matcher.find()) {
                            matched.put(matcher.start(), matcher.end());
                        }
                    }
                    if (!matched.isEmpty()) {
                        replaceTo = replacementsWithRegex.getReplaceTo();
                        replaceIndex = matched;
                    }
                }
            }
        }

        if (config.getConvertCase() != null) {
            switch (config.getConvertCase()) {
                case UPPER:
                    tableName = tableName.toUpperCase();
                    break;
                case LOWER:
                    tableName = tableName.toLowerCase();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported convert case: " + config.getConvertCase());
            }
        }
        int offset = 0;
        for (Map.Entry<Integer, Integer> index : replaceIndex.entrySet()) {
            int indexStart = index.getKey();
            int indexEnd = index.getValue();
            tableName =
                    tableName.substring(0, indexStart + offset)
                            + replaceTo.trim()
                            + tableName.substring(indexEnd + offset);
            offset += replaceTo.trim().length() - (indexEnd - indexStart);
        }
        if (StringUtils.isNotBlank(config.getPrefix())) {
            tableName = config.getPrefix().trim() + tableName;
        }
        if (StringUtils.isNotBlank(config.getSuffix())) {
            tableName = tableName + config.getSuffix().trim();
        }
        return tableName;
    }

    private Optional<String> getSpecificModify(String tableName) {
        if (!specificMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return Optional.of(specificMap.get(tableName));
    }

    private void preCheckForConfig(List<CatalogTable> inputCatalogTable) {
        if (config.getSpecific() == null || config.getSpecific().isEmpty()) {
            return;
        }

        Set<String> upstreamInputTableNames =
                inputCatalogTable.stream()
                        .map(e -> e.getTablePath().getTableName())
                        .collect(Collectors.toSet());

        List<String> notExistTables =
                config.getSpecific().stream()
                        .map(s -> s.getTableName())
                        .filter(t -> !upstreamInputTableNames.contains(t))
                        .collect(Collectors.toList());
        if (!notExistTables.isEmpty()) {
            throw TransformCommonError.getCatalogTableWithNotExistTables(
                    PLUGIN_NAME, notExistTables);
        }
    }
}
