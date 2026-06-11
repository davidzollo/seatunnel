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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableRenamerTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "TableRenamer";
    private List<CatalogTable> inputCatalogTable;
    private final TableRenamerConfig config;
    /** Keeps the renamed table path for each upstream full table name. */
    private final Map<String, TablePath> tablePathMapping = new HashMap<>();

    /** Keeps row-level table id rewrites aligned with produced catalog tables. */
    private final Map<String, String> tableIdMapping = new HashMap<>();

    /** Stores exact specific rules, optionally scoped by database. */
    private final Map<String, String> specificMap;

    public TableRenamerTransform(List<CatalogTable> inputCatalogTable, TableRenamerConfig config) {
        this.inputCatalogTable =
                inputCatalogTable.stream().map(CatalogTable::copy).collect(Collectors.toList());
        this.config = config;
        this.specificMap = initSpecificMap(config.getSpecific());
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
    public List<CatalogTable> getProducedCatalogTables() {
        preCheckForConfig(inputCatalogTable);
        tablePathMapping.clear();
        tableIdMapping.clear();

        List<CatalogTable> outputCatalogTable = new ArrayList<>();
        for (CatalogTable table : inputCatalogTable) {
            TablePath oldTablePath = table.getTablePath();
            String newName = convertName(oldTablePath);
            String schemaName = oldTablePath.getSchemaName();
            String newTableName = newName;
            if (newName.contains(".")) {
                String[] split = newName.split("\\.", 2);
                schemaName = split[0];
                newTableName = split[1];
            }
            TablePath newTablePath =
                    TablePath.of(table.getTableId().getDatabaseName(), schemaName, newTableName);
            CatalogTable newCatalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(table.getTableId().getCatalogName(), newTablePath),
                            table);
            outputCatalogTable.add(newCatalogTable);
            tablePathMapping.put(oldTablePath.getFullName(), newTablePath);
            tableIdMapping.put(oldTablePath.getFullName(), newTablePath.getFullName());
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
        TablePath newTablePath = tablePathMapping.get(tablePath.getFullName());
        if (newTablePath == null || newTablePath.equals(tablePath)) {
            return event;
        }

        if (event instanceof AlterTableColumnsEvent) {
            TableIdentifier newTableIdentifier =
                    TableIdentifier.of(event.tableIdentifier().getCatalogName(), newTablePath);
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
        TablePath newTablePath = tablePathMapping.get(tablePath.getFullName());
        if (newTablePath == null || newTablePath.equals(tablePath)) {
            return event;
        }

        TableIdentifier newTableIdentifier =
                TableIdentifier.of(event.tableIdentifier().getCatalogName(), newTablePath);
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
        Optional<String> specificValue = getSpecificModify(null, tableName, tableName);
        if (specificValue.isPresent()) {
            return specificValue.get();
        }
        return applyGlobalRename(tableName);
    }

    /**
     * Applies specific table matching with database-aware exact lookup first and leaf fallback
     * second, then reuses the legacy global rename pipeline.
     */
    private String convertName(TablePath tablePath) {
        Optional<String> specificValue = getSpecificModify(tablePath);
        if (specificValue.isPresent()) {
            return specificValue.get();
        }
        return applyGlobalRename(tablePath.getSchemaAndTableName());
    }

    /** Applies the original non-specific rename pipeline. */
    private String applyGlobalRename(String tableName) {
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

    /** Resolves a specific rule for the given table path with database-aware fallback order. */
    private Optional<String> getSpecificModify(TablePath tablePath) {
        return getSpecificModify(
                tablePath.getDatabaseName(),
                tablePath.getSchemaAndTableName(),
                tablePath.getTableName());
    }

    /** Resolves a specific rule by exact table name first and leaf table name second. */
    private Optional<String> getSpecificModify(
            String databaseName, String schemaAndTableName, String tableName) {
        Optional<String> scopedExact = getSpecificModifyByKey(databaseName, schemaAndTableName);
        if (scopedExact.isPresent()) {
            return scopedExact;
        }

        Optional<String> scopedLeaf = getSpecificModifyByKey(databaseName, tableName);
        if (scopedLeaf.isPresent()) {
            return scopedLeaf;
        }

        Optional<String> legacyExact = getSpecificModifyByKey(null, schemaAndTableName);
        if (legacyExact.isPresent()) {
            return legacyExact;
        }

        return getSpecificModifyByKey(null, tableName);
    }

    /** Looks up a specific rule using the normalized scope key. */
    private Optional<String> getSpecificModifyByKey(String databaseName, String tableName) {
        String specificKey = buildSpecificKey(databaseName, tableName);
        if (specificKey == null || !specificMap.containsKey(specificKey)) {
            return Optional.empty();
        }
        return Optional.of(specificMap.get(specificKey));
    }

    /** Builds the in-memory lookup map for specific rules with duplicate scope validation. */
    private Map<String, String> initSpecificMap(
            List<TableRenamerConfig.SpecificModify> specificModifies) {
        if (CollectionUtils.isEmpty(specificModifies)) {
            return Collections.emptyMap();
        }

        Map<String, String> scopedSpecificMap = new HashMap<>();
        for (TableRenamerConfig.SpecificModify specificModify : specificModifies) {
            if (specificModify == null || StringUtils.isBlank(specificModify.getTableName())) {
                continue;
            }
            String specificKey =
                    buildSpecificKey(specificModify.getDatabase(), specificModify.getTableName());
            String existedTarget =
                    scopedSpecificMap.putIfAbsent(specificKey, specificModify.getTargetName());
            if (existedTarget != null) {
                throw TransformCommonError.configValidationFailed(
                        PLUGIN_NAME,
                        String.format(
                                "Duplicate specific rule for database '%s' and table '%s'",
                                StringUtils.defaultIfBlank(specificModify.getDatabase(), "*"),
                                specificModify.getTableName()));
            }
        }
        return scopedSpecificMap;
    }

    /**
     * Builds the specific rule scope key and keeps legacy table-only matching when database is
     * absent.
     */
    private String buildSpecificKey(String databaseName, String tableName) {
        String normalizedTableName = StringUtils.trimToNull(tableName);
        if (normalizedTableName == null) {
            return null;
        }
        String normalizedDatabaseName = StringUtils.trimToNull(databaseName);
        if (normalizedDatabaseName == null) {
            return normalizedTableName;
        }
        return normalizedDatabaseName + "|" + normalizedTableName;
    }

    private void preCheckForConfig(List<CatalogTable> inputCatalogTable) {
        if (config.getSpecific() == null || config.getSpecific().isEmpty()) {
            return;
        }

        List<String> notExistTables =
                config.getSpecific().stream()
                        .filter(s -> s != null && StringUtils.isNotBlank(s.getTableName()))
                        .filter(
                                specificModify ->
                                        inputCatalogTable.stream()
                                                .map(CatalogTable::getTablePath)
                                                .noneMatch(
                                                        tablePath ->
                                                                matchesSpecificRule(
                                                                        tablePath, specificModify)))
                        .map(this::buildSpecificScopeName)
                        .collect(Collectors.toList());
        if (!notExistTables.isEmpty()) {
            throw TransformCommonError.getCatalogTableWithNotExistTables(
                    PLUGIN_NAME, notExistTables);
        }
    }

    /**
     * Matches a specific rule against the exact schema.table form first and leaf table form second.
     */
    private boolean matchesSpecificRule(
            TablePath tablePath, TableRenamerConfig.SpecificModify specificModify) {
        if (StringUtils.isNotBlank(specificModify.getDatabase())
                && !StringUtils.equals(specificModify.getDatabase(), tablePath.getDatabaseName())) {
            return false;
        }

        return StringUtils.equals(specificModify.getTableName(), tablePath.getSchemaAndTableName())
                || StringUtils.equals(specificModify.getTableName(), tablePath.getTableName());
    }

    /** Builds a readable scoped name for config validation errors. */
    private String buildSpecificScopeName(TableRenamerConfig.SpecificModify specificModify) {
        if (StringUtils.isBlank(specificModify.getDatabase())) {
            return specificModify.getTableName();
        }
        return specificModify.getDatabase() + "." + specificModify.getTableName();
    }
}
