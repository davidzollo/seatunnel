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

package org.apache.seatunnel.transform.fieldrenamer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.exception.FieldRenamerError;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformExceptionUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FieldRenamerTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "FieldRenamer";
    private List<CatalogTable> inputCatalogTable;
    private final FieldRenamerConfig config;

    public FieldRenamerTransform(List<CatalogTable> inputCatalogTable, FieldRenamerConfig config) {
        this.inputCatalogTable =
                inputCatalogTable.stream().map(CatalogTable::copy).collect(Collectors.toList());
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> outputCatalogTable = new ArrayList<>();
        Map<String, Map<String, String>> tableWithDuplicateName = new LinkedHashMap<>();
        preCheckForSpecificConfig(inputCatalogTable);
        for (CatalogTable table : inputCatalogTable) {
            CatalogTable newCatalogTable;
            String tableName = table.getTablePath().getFullName();
            if (shouldBeRenamed(tableName)) {
                Map<String, String> changedName = new LinkedHashMap<>();
                List<Column> newColumns = new ArrayList<>();
                for (Column column : table.getTableSchema().getColumns()) {
                    String newName = convertName(tableName, column.getName());
                    changedName.put(column.getName(), newName);
                    newColumns.add(column.rename(newName));
                }
                Map<String, String> duplicated =
                        changedName.entrySet().stream()
                                .collect(
                                        Collectors.groupingBy(
                                                Map.Entry::getValue, Collectors.toList()))
                                .values()
                                .stream()
                                .filter(l -> l.size() > 1)
                                .flatMap(List::stream)
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                Map.Entry::getValue,
                                                (oldValue, newValue) -> newValue,
                                                LinkedHashMap::new));
                if (!duplicated.isEmpty()) {
                    tableWithDuplicateName.put(tableName, duplicated);
                }
                newCatalogTable = getNewCatalogTableByChangedName(table, changedName, newColumns);
            } else {
                newCatalogTable = table.copy();
            }
            outputCatalogTable.add(newCatalogTable);
        }
        if (!tableWithDuplicateName.isEmpty()) {
            throw FieldRenamerError.tableDuplicateFieldNameError(tableWithDuplicateName);
        }
        return outputCatalogTable;
    }

    private boolean shouldBeRenamed(String tableName) {
        return shouldBeRenamedByTableList(tableName)
                || shouldBeRenamedByRegex(tableName)
                || shouldBeRenamedBySpecific(tableName);
    }

    private void preCheckForSpecificConfig(List<CatalogTable> inputCatalogTable) {
        if (config.getSpecific() == null || config.getSpecific().isEmpty()) {
            return;
        }
        Map<String, Set<String>> tableFields = new LinkedHashMap<>();
        for (CatalogTable table : inputCatalogTable) {
            String tableName = table.getTablePath().getFullName();
            Set<String> fields =
                    table.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toSet());
            tableFields.put(tableName, fields);
        }
        TransformExceptionUtil.withErrorCheck(
                PLUGIN_NAME,
                config.getSpecific().iterator(),
                entry -> {
                    String tableName = entry.getTableName();
                    String field = entry.getFieldName();
                    if (!tableFields.containsKey(tableName)) {
                        throw TransformCommonError.cannotFindInputTableError(
                                PLUGIN_NAME, tableName);
                    }
                    if (!tableFields.get(tableName).contains(field)) {
                        throw TransformCommonError.cannotFindInputTableFieldError(
                                PLUGIN_NAME, tableName, field);
                    }
                });
    }

    private static CatalogTable getNewCatalogTableByChangedName(
            CatalogTable table, Map<String, String> changedName, List<Column> newColumns) {
        CatalogTable newCatalogTable;
        List<String> newPrimaryKey = null;
        if (table.getTableSchema().getPrimaryKey() != null
                && table.getTableSchema().getPrimaryKey().getColumnNames() != null) {
            newPrimaryKey =
                    table.getTableSchema().getPrimaryKey().getColumnNames().stream()
                            .map(key -> changedName.getOrDefault(key, key))
                            .collect(Collectors.toList());
        }
        List<ConstraintKey> newConstraintKeys =
                table.getTableSchema().getConstraintKeys().stream()
                        .map(
                                key -> {
                                    List<ConstraintKey.ConstraintKeyColumn> columnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            column ->
                                                                    ConstraintKey
                                                                            .ConstraintKeyColumn.of(
                                                                            changedName
                                                                                    .getOrDefault(
                                                                                            column
                                                                                                    .getColumnName(),
                                                                                            column
                                                                                                    .getColumnName()),
                                                                            column.getSortType()))
                                                    .collect(Collectors.toList());
                                    return ConstraintKey.of(
                                            key.getConstraintType(),
                                            key.getConstraintName(),
                                            columnNames);
                                })
                        .collect(Collectors.toList());
        TableSchema newTableSchema =
                TableSchema.builder()
                        .columns(newColumns)
                        .primaryKey(
                                newPrimaryKey == null
                                        ? null
                                        : PrimaryKey.of(
                                                table.getTableSchema()
                                                        .getPrimaryKey()
                                                        .getPrimaryKey(),
                                                newPrimaryKey))
                        .constraintKey(newConstraintKeys)
                        .build();
        List<String> newPartitionKeys =
                table.getPartitionKeys().stream()
                        .map(key -> changedName.getOrDefault(key, key))
                        .collect(Collectors.toList());
        newCatalogTable =
                CatalogTable.of(
                        table.getTableId().copy(),
                        newTableSchema,
                        new HashMap<>(table.getOptions()),
                        newPartitionKeys,
                        table.getComment(),
                        table.getCatalogName());
        return newCatalogTable;
    }

    @VisibleForTesting
    public String convertName(String tableName, String name) {
        if (name == null) {
            return null;
        }
        Optional<FieldRenamerConfig.SpecificModify> specificValue =
                getSpecificModify(tableName, name);
        if (specificValue.isPresent()) {
            return specificValue.get().getTargetName();
        }
        if (!shouldBeRenamedByRegex(tableName)) {
            return name;
        }
        String replaceFrom = null;
        String replaceTo = null;
        Map<Integer, Integer> replaceIndex = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(config.getReplacementsWithRegex())) {
            for (FieldRenamerConfig.ReplacementsWithRegex replacementsWithRegex :
                    config.getReplacementsWithRegex()) {
                Boolean isRegex = replacementsWithRegex.getIsRegex();
                String replacement = replacementsWithRegex.getReplaceFrom();
                if (StringUtils.isNotEmpty(replacement)) {
                    Map<Integer, Integer> matched = new LinkedHashMap<>();
                    if (BooleanUtils.isNotTrue(isRegex)) {
                        if (StringUtils.equals(replacement, name)) {
                            matched.put(0, name.length());
                        }
                    } else {
                        Matcher matcher = Pattern.compile(replacement).matcher(name);
                        while (matcher.find()) {
                            matched.put(matcher.start(), matcher.end());
                        }
                    }
                    if (!matched.isEmpty()) {
                        replaceFrom = replacement;
                        replaceTo = replacementsWithRegex.getReplaceTo();
                        replaceIndex = matched;
                    }
                }
            }
        }

        if (config.getConvertCase() != null) {
            switch (config.getConvertCase()) {
                case UPPER:
                    name = name.toUpperCase();
                    break;
                case LOWER:
                    name = name.toLowerCase();
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
            name =
                    name.substring(0, indexStart + offset)
                            + replaceTo.trim()
                            + name.substring(indexEnd + offset);
            offset += replaceTo.trim().length() - (indexEnd - indexStart);
        }
        if (StringUtils.isNotBlank(config.getPrefix())) {
            name = config.getPrefix().trim() + name;
        }
        if (StringUtils.isNotBlank(config.getSuffix())) {
            name = name + config.getSuffix().trim();
        }
        return name;
    }

    private boolean shouldBeRenamedBySpecific(String tableName) {
        return config.getSpecific().stream()
                .anyMatch(specific -> specific.getTableName().equals(tableName));
    }

    private boolean shouldBeRenamedByRegex(String tableName) {
        if (StringUtils.isNotBlank(config.getTableMatchRegex())) {
            return TablePath.of(tableName).getTableName().matches(config.getTableMatchRegex());
        } else {
            return true;
        }
    }

    private boolean shouldBeRenamedByTableList(String tableName) {
        if (CollectionUtils.isNotEmpty(config.getMatchTables())) {
            return config.getMatchTables().contains(tableName);
        }
        return false;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        return row;
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
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof AlterTableChangeColumnEvent) {
            AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                    (AlterTableChangeColumnEvent) schemaChangeEvent;
            Column newColumn = alterTableChangeColumnEvent.getColumn();
            String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
            inputCatalogTable.stream()
                    .filter(
                            t ->
                                    t.getTablePath()
                                            .equals(alterTableChangeColumnEvent.getTablePath()))
                    .forEach(
                            t -> {
                                t.getTableSchema()
                                        .getColumns()
                                        .removeIf(c -> c.getName().equals(oldColumnName));
                                t.getTableSchema().getColumns().add(newColumn);
                            });
            String tableName = alterTableChangeColumnEvent.getTablePath().getFullName();
            boolean uselessEvent = false;
            if (shouldBeRenamedBySpecific(tableName)) {
                Optional<FieldRenamerConfig.SpecificModify> specificValue =
                        getSpecificModify(tableName, oldColumnName);
                if (specificValue.isPresent()) {
                    List<FieldRenamerConfig.SpecificModify> newSpecificList =
                            new ArrayList<>(config.getSpecific());
                    newSpecificList.remove(specificValue.get());
                    newSpecificList.add(
                            new FieldRenamerConfig.SpecificModify(
                                    tableName,
                                    newColumn.getName(),
                                    specificValue.get().getTargetName()));
                    config.setSpecific(newSpecificList);
                    uselessEvent = true;
                }
            }
            // Use getProducedCatalogTables() to check rules
            getProducedCatalogTables();
            if (uselessEvent) {
                return null;
            }
            if (shouldBeRenamed(tableName)) {
                Column columnRenamed =
                        alterTableChangeColumnEvent
                                .getColumn()
                                .rename(
                                        convertName(
                                                tableName,
                                                alterTableChangeColumnEvent.getColumn().getName()));
                String oldColumnRenamed =
                        convertName(tableName, alterTableChangeColumnEvent.getOldColumn());
                String afterColumnRenamed =
                        convertName(tableName, alterTableChangeColumnEvent.getAfterColumn());
                return new AlterTableChangeColumnEvent(
                        alterTableChangeColumnEvent.tableIdentifier(),
                        oldColumnRenamed,
                        columnRenamed,
                        alterTableChangeColumnEvent.isFirst(),
                        afterColumnRenamed);
            }
        } else if (schemaChangeEvent instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent alterTableAddColumnEvent =
                    (AlterTableAddColumnEvent) schemaChangeEvent;
            Column newColumn = alterTableAddColumnEvent.getColumn();
            inputCatalogTable.stream()
                    .filter(t -> t.getTablePath().equals(alterTableAddColumnEvent.getTablePath()))
                    .forEach(t -> t.getTableSchema().getColumns().add(newColumn));
            // Use getProducedCatalogTables() to check rules
            getProducedCatalogTables();
            String tableName = alterTableAddColumnEvent.getTablePath().getFullName();
            if (shouldBeRenamed(tableName)) {
                Column columnRenamed =
                        alterTableAddColumnEvent
                                .getColumn()
                                .rename(
                                        convertName(
                                                tableName,
                                                alterTableAddColumnEvent.getColumn().getName()));
                String afterColumnRenamed =
                        convertName(tableName, alterTableAddColumnEvent.getAfterColumn());
                return new AlterTableAddColumnEvent(
                        alterTableAddColumnEvent.tableIdentifier(),
                        columnRenamed,
                        alterTableAddColumnEvent.isFirst(),
                        afterColumnRenamed);
            }
        } else if (schemaChangeEvent instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent alterTableDropColumnEvent =
                    (AlterTableDropColumnEvent) schemaChangeEvent;
            String oldColumnName = alterTableDropColumnEvent.getColumn();
            inputCatalogTable.stream()
                    .filter(t -> t.getTablePath().equals(alterTableDropColumnEvent.getTablePath()))
                    .forEach(
                            t ->
                                    t.getTableSchema()
                                            .getColumns()
                                            .removeIf(c -> c.getName().equals(oldColumnName)));
            // Use getProducedCatalogTables() to check rules
            getProducedCatalogTables();
            String tableName = alterTableDropColumnEvent.getTablePath().getFullName();
            if (shouldBeRenamed(tableName)) {
                String oldColumnRenamed =
                        convertName(tableName, alterTableDropColumnEvent.getColumn());
                return new AlterTableDropColumnEvent(
                        alterTableDropColumnEvent.tableIdentifier(), oldColumnRenamed);
            }
        } else if (schemaChangeEvent instanceof AlterTableNameEvent) {
            AlterTableNameEvent alterTableNameEvent = (AlterTableNameEvent) schemaChangeEvent;
            TablePath newTableName = alterTableNameEvent.getNewTablePath();
            TablePath oldTableName = alterTableNameEvent.getTablePath();
            inputCatalogTable =
                    inputCatalogTable.stream()
                            .map(
                                    t -> {
                                        if (t.getTablePath().equals(oldTableName)) {
                                            return CatalogTable.of(
                                                    TableIdentifier.of(
                                                            t.getCatalogName(), newTableName),
                                                    t);
                                        } else {
                                            return t;
                                        }
                                    })
                            .collect(Collectors.toList());
            if (config.getSpecific() != null) {
                List<FieldRenamerConfig.SpecificModify> newSpecificList =
                        new ArrayList<>(config.getSpecific());
                newSpecificList.replaceAll(
                        specific -> {
                            if (specific.getTableName().equals(oldTableName.getFullName())) {
                                return new FieldRenamerConfig.SpecificModify(
                                        newTableName.getFullName(),
                                        specific.getFieldName(),
                                        specific.getTargetName());
                            } else {
                                return specific;
                            }
                        });
                config.setSpecific(newSpecificList);
            }
            // Use getProducedCatalogTables() to check rules
            getProducedCatalogTables();
        } else if (schemaChangeEvent instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent =
                    (AlterTableColumnsEvent) schemaChangeEvent;
            List<AlterTableColumnEvent> events =
                    alterTableColumnsEvent.getEvents().stream()
                            .map(this::mapSchemaChangeEvent)
                            .filter(Objects::nonNull)
                            .map(e -> (AlterTableColumnEvent) e)
                            .collect(Collectors.toList());
            if (events.isEmpty()) {
                return null;
            }
            return new AlterTableColumnsEvent(alterTableColumnsEvent.getTableIdentifier(), events);
        }
        return schemaChangeEvent;
    }

    private Optional<FieldRenamerConfig.SpecificModify> getSpecificModify(
            String tableName, String oldColumnName) {
        if (config.getSpecific() == null) {
            return Optional.empty();
        }
        return config.getSpecific().stream()
                .filter(specific -> specific.getTableName().equals(tableName))
                .filter(specific -> specific.getFieldName().equals(oldColumnName))
                .findFirst();
    }
}
