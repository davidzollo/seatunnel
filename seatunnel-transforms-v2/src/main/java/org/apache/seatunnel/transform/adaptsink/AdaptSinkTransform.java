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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.adaptsink.AdaptSinkTableCasts.castColumnData;
import static org.apache.seatunnel.transform.adaptsink.AdaptSinkTableCasts.tryCastColumnType;

public class AdaptSinkTransform implements SeaTunnelTransform<SeaTunnelRow> {

    public static String PLUGIN_NAME = "AdaptSink";

    private Map<String, CatalogTable> inputTables;
    private Map<String, CatalogTable> outputTables;
    private AdaptSinkTransformConfig config;
    private TableSchemaChangeEventHandler schemaChangeEventHandler;

    public AdaptSinkTransform(
            Map<String, CatalogTable> inputTables, AdaptSinkTransformConfig config) {
        this.inputTables = inputTables;
        this.config = config;
        this.outputTables = castTable(inputTables, config.getSinkTables());
        this.schemaChangeEventHandler = new TableSchemaChangeEventDispatcher();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return getProducedCatalogTable().getSeaTunnelRowType();
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return getProducedCatalogTables().get(0);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(outputTables.values());
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        String tableKey = schemaChangeEvent.tablePath().toString();
        CatalogTable inputTable = inputTables.get(tableKey);
        inputTable =
                CatalogTable.of(
                        inputTable.getTableId(),
                        schemaChangeEventHandler
                                .reset(inputTable.getTableSchema())
                                .apply(schemaChangeEvent),
                        inputTable.getOptions(),
                        inputTable.getPartitionKeys(),
                        inputTable.getComment());
        inputTables.put(tableKey, inputTable);

        schemaChangeEvent = convertSchemaChangeEvent(schemaChangeEvent);
        if (schemaChangeEvent == null) {
            // skip
            return null;
        }

        CatalogTable outputTable = outputTables.get(tableKey);
        outputTable =
                CatalogTable.of(
                        outputTable.getTableId(),
                        schemaChangeEventHandler
                                .reset(outputTable.getTableSchema())
                                .apply(schemaChangeEvent),
                        outputTable.getOptions(),
                        outputTable.getPartitionKeys(),
                        outputTable.getComment());
        outputTables.put(tableKey, outputTable);
        return schemaChangeEvent;
    }

    private SchemaChangeEvent convertSchemaChangeEvent(SchemaChangeEvent event) {
        String tableKey = event.tablePath().toString();
        CatalogTable outputTable = outputTables.get(tableKey);

        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
            AlterTableColumnsEvent mergedColumns =
                    new AlterTableColumnsEvent(columnsEvent.tableIdentifier());
            for (AlterTableColumnEvent columnEvent : columnsEvent.getEvents()) {
                AlterTableColumnEvent mergedColumn =
                        (AlterTableColumnEvent) convertSchemaChangeEvent(columnEvent);
                if (mergedColumn != null) {
                    mergedColumns.addEvent(mergedColumn);
                }
            }
            event = mergedColumns.getEvents().isEmpty() ? null : mergedColumns;
        } else if (event instanceof AlterTableChangeColumnEvent) {
            AlterTableChangeColumnEvent changeColumnEvent = ((AlterTableChangeColumnEvent) event);
            if (!outputTable.getTableSchema().contains(changeColumnEvent.getOldColumn())) {
                // skip
                return null;
            }

            if (config.isAdaptSinkTableType()) {
                boolean unRename =
                        changeColumnEvent
                                .getOldColumn()
                                .equals(changeColumnEvent.getColumn().getName());
                if (unRename) {
                    // skip
                    return null;
                }

                event =
                        AlterTableChangeColumnEvent.change(
                                changeColumnEvent.tableIdentifier(),
                                changeColumnEvent.getOldColumn(),
                                outputTable
                                        .getTableSchema()
                                        .getColumn(changeColumnEvent.getOldColumn())
                                        .rename(changeColumnEvent.getColumn().getName()));
            }
        } else if (event instanceof AlterTableModifyColumnEvent) {
            if (config.isAdaptSinkTableType()) {
                // skip
                return null;
            }

            AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
            if (!outputTable.getTableSchema().contains(modifyColumnEvent.getColumn().getName())) {
                // skip
                return null;
            }
        } else if (event instanceof AlterTableAddColumnEvent) {
            if (config.isAdaptSinkTableColumns()) {
                // skip
                return null;
            }
        } else if (event instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
            if (!outputTable.getTableSchema().contains(dropColumnEvent.getColumn())) {
                // skip
                return null;
            }
        }
        return event;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow inputRow) {
        CatalogTable inputTable = inputTables.get(inputRow.getTableId());
        CatalogTable outputTable = outputTables.get(inputRow.getTableId());
        if (outputTable == null) {
            return inputRow;
        }

        return castRowData(inputTable, outputTable, inputRow);
    }

    private SeaTunnelRow castRowData(
            CatalogTable inputTable, CatalogTable outputTable, SeaTunnelRow inputRow) {
        List<Object> fields = new ArrayList<>();
        for (int i = 0; i < inputRow.getArity(); i++) {
            Column inputColumn = inputTable.getTableSchema().getColumns().get(i);
            boolean exitsOfOutputTable =
                    outputTable.getTableSchema().contains(inputColumn.getName());
            if (!exitsOfOutputTable) {
                if (!config.isAdaptSinkTableColumns()) {
                    fields.add(inputRow.getField(i));
                }
                continue;
            }

            Column outputColumn = outputTable.getTableSchema().getColumn(inputColumn.getName());
            if (inputColumn.getDataType().equals(outputColumn.getDataType())
                    || !config.isAdaptSinkTableType()) {
                fields.add(inputRow.getField(i));
                continue;
            }

            Object castColumnValue =
                    castColumnData(inputColumn, outputColumn, inputRow.getField(i));
            fields.add(castColumnValue);
        }

        SeaTunnelRow outputRow = new SeaTunnelRow(fields.toArray());
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setRowKind(inputRow.getRowKind());
        return outputRow;
    }

    private Map<String, CatalogTable> castTable(
            Map<String, CatalogTable> inputTables, Map<String, CatalogTable> outputTables) {
        Map<String, CatalogTable> mergedTables = new LinkedHashMap<>();
        for (String key : inputTables.keySet()) {
            CatalogTable inputTable = inputTables.get(key);
            CatalogTable outputTable = outputTables.get(key);
            if (outputTable == null) {
                mergedTables.put(key, inputTable);
                continue;
            }

            List<Column> mergedColumns = new ArrayList<>();
            for (String column : inputTable.getTableSchema().getColumnNames()) {
                Column inputColumn = inputTable.getTableSchema().getColumn(column);
                boolean exitsOfOutputTable = outputTable.getTableSchema().contains(column);
                if (!exitsOfOutputTable) {
                    if (!config.isAdaptSinkTableColumns()) {
                        mergedColumns.add(inputColumn);
                    }
                    continue;
                }

                Column outputColumn = outputTable.getTableSchema().getColumn(column);
                if (inputColumn.getDataType().equals(outputColumn.getDataType())
                        || !config.isAdaptSinkTableType()) {
                    mergedColumns.add(inputColumn);
                    continue;
                }

                tryCastColumnType(inputColumn, outputColumn);

                mergedColumns.add(inputColumn.copy(outputColumn.getDataType()));
            }

            CatalogTable mergedTable =
                    CatalogTable.of(
                            inputTable.getTableId(),
                            TableSchema.builder()
                                    .primaryKey(inputTable.getTableSchema().getPrimaryKey())
                                    .constraintKey(inputTable.getTableSchema().getConstraintKeys())
                                    .columns(mergedColumns)
                                    .build(),
                            inputTable.getOptions(),
                            inputTable.getPartitionKeys(),
                            inputTable.getComment());
            mergedTables.put(key, mergedTable);
        }
        return mergedTables;
    }
}
