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

package org.apache.seatunnel.transform.fieldremover;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
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
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformExceptionUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FieldRemoverTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "FieldRemover";
    private final List<CatalogTable> inputCatalogTable;
    private final Map<String, List<Integer>> newIndexes;
    private final FieldRemoverConfig config;

    public FieldRemoverTransform(List<CatalogTable> inputCatalogTable, FieldRemoverConfig config) {
        this.inputCatalogTable = inputCatalogTable;
        this.config = config;
        this.newIndexes = getNewIndexesIndexes(inputCatalogTable);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> outputCatalogTable = new ArrayList<>();
        preCheckForConfig(inputCatalogTable);
        for (CatalogTable table : inputCatalogTable) {
            CatalogTable newCatalogTable;
            String tableName = table.getTablePath().getFullName();
            if (shouldBeRemovedByConfig(tableName)) {
                List<String> deletedFields = new ArrayList<>();
                List<Column> newColumns = new ArrayList<>();
                for (Column column : table.getTableSchema().getColumns()) {
                    if (config.getRemovedFields().get(tableName).contains(column.getName())) {
                        deletedFields.add(column.getName());
                    } else {
                        newColumns.add(column);
                    }
                }
                newCatalogTable =
                        getNewCatalogTableByDeletedFields(table, deletedFields, newColumns);
            } else {
                newCatalogTable = table.copy();
            }
            outputCatalogTable.add(newCatalogTable);
        }
        return outputCatalogTable;
    }

    private Map<String, List<Integer>> getNewIndexesIndexes(List<CatalogTable> inputCatalogTable) {
        Map<String, List<Integer>> indexes = new HashMap<>();
        for (CatalogTable table : inputCatalogTable) {
            if (shouldBeRemovedByConfig(table.getTablePath().getFullName())) {
                List<String> columnNames =
                        table.getTableSchema().getColumns().stream()
                                .map(Column::getName)
                                .collect(Collectors.toList());
                List<Integer> removedIndex =
                        config.getRemovedFields().get(table.getTablePath().getFullName()).stream()
                                .map(columnNames::indexOf)
                                .collect(Collectors.toList());
                List<Integer> newIndex =
                        IntStream.range(0, columnNames.size())
                                .filter(i -> !removedIndex.contains(i))
                                .boxed()
                                .collect(Collectors.toList());
                indexes.put(table.getTablePath().getFullName(), newIndex);
            }
        }
        return indexes;
    }

    private void preCheckForConfig(List<CatalogTable> inputCatalogTable) {
        Map<String, Set<String>> tableFields = new LinkedHashMap<>();
        for (CatalogTable table : inputCatalogTable) {
            String tableName = table.getTablePath().getFullName();
            Set<String> fields =
                    table.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toSet());
            tableFields.put(tableName, fields);
        }
        List<AbstractMap.SimpleEntry<String, String>> fields =
                config.getRemovedFields().entrySet().stream()
                        .flatMap(
                                entry ->
                                        entry.getValue().stream()
                                                .map(
                                                        value ->
                                                                new AbstractMap.SimpleEntry<>(
                                                                        entry.getKey(), value)))
                        .collect(Collectors.toList());
        TransformExceptionUtil.withErrorCheck(
                PLUGIN_NAME,
                fields.iterator(),
                entry -> {
                    String tableName = entry.getKey();
                    String field = entry.getValue();
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

    private static CatalogTable getNewCatalogTableByDeletedFields(
            CatalogTable table, List<String> deletedFields, List<Column> newColumns) {
        CatalogTable newCatalogTable;
        List<String> newPrimaryKey = null;
        if (table.getTableSchema().getPrimaryKey() != null
                && table.getTableSchema().getPrimaryKey().getColumnNames() != null) {
            newPrimaryKey =
                    table.getTableSchema().getPrimaryKey().getColumnNames().stream()
                            .filter(key -> !deletedFields.contains(key))
                            .collect(Collectors.toList());
        }
        List<ConstraintKey> newConstraintKeys =
                table.getTableSchema().getConstraintKeys().stream()
                        .map(
                                key -> {
                                    List<ConstraintKey.ConstraintKeyColumn> columnNames =
                                            key.getColumnNames().stream()
                                                    .filter(
                                                            column ->
                                                                    !deletedFields.contains(
                                                                            column.getColumnName()))
                                                    .collect(Collectors.toList());
                                    return ConstraintKey.of(
                                            key.getConstraintType(),
                                            key.getConstraintName(),
                                            columnNames);
                                })
                        .filter(key -> !key.getColumnNames().isEmpty())
                        .collect(Collectors.toList());
        TableSchema newTableSchema =
                TableSchema.builder()
                        .columns(newColumns)
                        .primaryKey(
                                newPrimaryKey == null || newPrimaryKey.isEmpty()
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
                        .filter(key -> !deletedFields.contains(key))
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

    private boolean shouldBeRemovedByConfig(String tableName) {
        return config.getRemovedFields().containsKey(tableName);
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        if (shouldBeRemovedByConfig(row.getTableId())) {
            row =
                    row.copy(
                            newIndexes.get(row.getTableId()).stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray());
        }
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
            String tableName = alterTableChangeColumnEvent.getTablePath().getFullName();
            if (shouldBeRemovedByConfig(tableName)) {
                if (config.getRemovedFields().get(tableName).contains(oldColumnName)
                        || config.getRemovedFields().get(tableName).contains(newColumn.getName())) {
                    return null;
                }
            }
        } else if (schemaChangeEvent instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent alterTableAddColumnEvent =
                    (AlterTableAddColumnEvent) schemaChangeEvent;
            Column newColumn = alterTableAddColumnEvent.getColumn();
            String tableName = alterTableAddColumnEvent.getTablePath().getFullName();
            if (shouldBeRemovedByConfig(tableName)) {
                if (config.getRemovedFields().get(tableName).contains(newColumn.getName())) {
                    return null;
                }
            }
        } else if (schemaChangeEvent instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent alterTableDropColumnEvent =
                    (AlterTableDropColumnEvent) schemaChangeEvent;
            String oldColumnName = alterTableDropColumnEvent.getColumn();
            String tableName = alterTableDropColumnEvent.getTablePath().getFullName();
            if (shouldBeRemovedByConfig(tableName)) {
                if (config.getRemovedFields().get(tableName).contains(oldColumnName)) {
                    return null;
                }
            }
        } else if (schemaChangeEvent instanceof AlterTableNameEvent) {
            AlterTableNameEvent alterTableNameEvent = (AlterTableNameEvent) schemaChangeEvent;
            String newTableName = alterTableNameEvent.getNewTablePath().getFullName();
            String oldTableName = alterTableNameEvent.getTablePath().getFullName();
            if (shouldBeRemovedByConfig(oldTableName)) {
                List<String> removedFields = config.getRemovedFields().remove(oldTableName);
                config.getRemovedFields().put(newTableName, removedFields);
            }
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
            } else {
                return new AlterTableColumnsEvent(
                        alterTableColumnsEvent.getTableIdentifier(), events);
            }
        }
        return schemaChangeEvent;
    }
}
