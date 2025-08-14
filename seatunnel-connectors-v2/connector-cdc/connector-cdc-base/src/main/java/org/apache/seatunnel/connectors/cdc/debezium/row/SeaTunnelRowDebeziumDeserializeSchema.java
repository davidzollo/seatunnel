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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.AbstractDebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeAfterWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeBeforeWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** Deserialization schema from Debezium object to {@link SeaTunnelRow}. */
@Slf4j
public final class SeaTunnelRowDebeziumDeserializeSchema
        extends AbstractDebeziumDeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_TABLE_NAME_KEY = null;

    private final MetadataConverter[] metadataConverters;
    private final ZoneId serverTimeZone;
    private final DebeziumDeserializationConverterFactory userDefinedConverterFactory;
    private final SchemaChangeResolver schemaChangeResolver;
    private final TableSchemaChangeEventHandler tableSchemaChangeHandler;
    private List<CatalogTable> tables;
    private Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters;

    private final Map<String, Boolean> selectAllMap;
    private final Map<String, List<String>> readColumnsMap;

    SeaTunnelRowDebeziumDeserializeSchema(
            MetadataConverter[] metadataConverters,
            List<CatalogTable> tables,
            ZoneId serverTimeZone,
            DebeziumDeserializationConverterFactory userDefinedConverterFactory,
            SchemaChangeResolver schemaChangeResolver,
            Map<TableId, Struct> tableIdTableChangeMap,
            Map<String, List<String>> readColumnsMap) {
        super(tableIdTableChangeMap);
        this.metadataConverters = metadataConverters;
        this.serverTimeZone = serverTimeZone;
        this.userDefinedConverterFactory = userDefinedConverterFactory;
        this.tables = checkNotNull(tables);
        this.schemaChangeResolver = schemaChangeResolver;
        this.tableSchemaChangeHandler = new TableSchemaChangeEventDispatcher();
        this.tableRowConverters =
                createTableRowConverters(
                        tables, metadataConverters, serverTimeZone, userDefinedConverterFactory);
        this.selectAllMap =
                tables.stream()
                        .map(CatalogTable::getTablePath)
                        .collect(
                                HashMap::new,
                                (map, tablePath) ->
                                        map.put(
                                                tablePath.toString(),
                                                readColumnsMap == null
                                                        || readColumnsMap.isEmpty()
                                                        || readColumnsMap.get(tablePath.toString())
                                                                == null),
                                HashMap::putAll);
        this.readColumnsMap = readColumnsMap;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SeaTunnelRow> collector)
            throws Exception {
        super.deserialize(record, collector);
        if (isSchemaChangeBeforeWatermarkEvent(record)) {
            collector.markSchemaChangeBeforeCheckpoint();
            return;
        }
        if (isSchemaChangeAfterWatermarkEvent(record)) {
            collector.markSchemaChangeAfterCheckpoint();
            return;
        }
        if (isSchemaChangeEvent(record)) {
            deserializeSchemaChangeRecord(record, collector);
            return;
        }

        if (isDataChangeRecord(record)) {
            deserializeDataChangeRecord(record, collector);
            return;
        }

        log.debug("Unsupported record {}, just skip.", record);
    }

    private void deserializeSchemaChangeRecord(
            SourceRecord record, Collector<SeaTunnelRow> collector) {
        SchemaChangeEvent schemaChangeEvent = schemaChangeResolver.resolve(record, tables);
        if (schemaChangeEvent == null) {
            log.info("Unsupported resolve schemaChangeEvent {}, just skip.", record);
            return;
        }

        boolean tableExist = false;
        for (int i = 0; i < tables.size(); i++) {
            CatalogTable changeBefore = tables.get(i);
            if (!schemaChangeEvent.tablePath().equals(changeBefore.getTablePath())) {
                continue;
            }

            tableExist = true;
            log.debug(
                    "Table[{}] change before: {}",
                    schemaChangeEvent.tablePath(),
                    changeBefore.getTableSchema());

            CatalogTable changeAfter = null;
            if (EventType.SCHEMA_CHANGE_UPDATE_COLUMNS.equals(schemaChangeEvent.getEventType())) {
                AlterTableColumnsEvent alterTableColumnsEvent =
                        (AlterTableColumnsEvent) schemaChangeEvent;
                for (AlterTableColumnEvent event : alterTableColumnsEvent.getEvents()) {
                    TableSchema changeAfterSchema =
                            tableSchemaChangeHandler
                                    .reset(changeBefore.getTableSchema())
                                    .apply(event);
                    changeAfter =
                            CatalogTable.of(
                                    changeBefore.getTableId(),
                                    changeAfterSchema,
                                    changeBefore.getOptions(),
                                    changeBefore.getPartitionKeys(),
                                    changeBefore.getComment());
                    event.setChangeAfter(changeAfter);

                    changeBefore = changeAfter;
                }
            } else {
                TableSchema changeAfterSchema =
                        tableSchemaChangeHandler
                                .reset(changeBefore.getTableSchema())
                                .apply(schemaChangeEvent);
                changeAfter =
                        CatalogTable.of(
                                changeBefore.getTableId(),
                                changeAfterSchema,
                                changeBefore.getOptions(),
                                changeBefore.getPartitionKeys(),
                                changeBefore.getComment());
            }
            tables.set(i, changeAfter);
            schemaChangeEvent.setChangeAfter(changeAfter);
            log.debug(
                    "Table[{}] change after: {}",
                    schemaChangeEvent.tablePath(),
                    changeAfter.getTableSchema());
            break;
        }
        if (!tableExist) {
            log.error(
                    "Not found table {}, skip schema change event {}",
                    schemaChangeEvent.tablePath());
        }
        tableRowConverters =
                createTableRowConverters(
                        tables, metadataConverters, serverTimeZone, userDefinedConverterFactory);
        collector.collect(schemaChangeEvent);
    }

    private void deserializeDataChangeRecord(SourceRecord record, Collector<SeaTunnelRow> collector)
            throws Exception {
        Envelope.Operation operation = Envelope.operationFor(record);
        Struct messageStruct = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        String tableId = tablePath.toString();
        SeaTunnelRowDebeziumDeserializationConverters converters;
        if (tables.size() > 1) {
            converters = tableRowConverters.get(tableId);
            if (converters == null) {
                log.debug("Ignore newly added table {}", tableId);
                return;
            }
        } else {
            converters = tableRowConverters.get(DEFAULT_TABLE_NAME_KEY);
        }

        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            SeaTunnelRow insert = extractAfterRow(converters, record, messageStruct, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            insert.setTableId(tableId);
            SeaTunnelRow filteredInsert = filterFields(insert, tableId);
            if (filteredInsert != null) {
                collector.collect(filteredInsert);
            }
        } else if (operation == Envelope.Operation.DELETE) {
            SeaTunnelRow delete = extractBeforeRow(converters, record, messageStruct, valueSchema);
            delete.setRowKind(RowKind.DELETE);
            delete.setTableId(tableId);
            SeaTunnelRow filteredDelete = filterFields(delete, tableId);
            if (filteredDelete != null) {
                collector.collect(filteredDelete);
            }
        } else {
            SeaTunnelRow before = extractBeforeRow(converters, record, messageStruct, valueSchema);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            before.setTableId(tableId);
            SeaTunnelRow filteredBefore = filterFields(before, tableId);
            if (filteredBefore != null) {
                collector.collect(filteredBefore);
            }

            SeaTunnelRow after = extractAfterRow(converters, record, messageStruct, valueSchema);
            after.setRowKind(RowKind.UPDATE_AFTER);
            after.setTableId(tableId);
            SeaTunnelRow filteredAfter = filterFields(after, tableId);
            if (filteredAfter != null) {
                collector.collect(filteredAfter);
            }
        }
    }

    private SeaTunnelRow extractAfterRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return runtimeConverter.convert(record, after, afterSchema);
    }

    private SeaTunnelRow extractBeforeRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return runtimeConverter.convert(record, before, beforeSchema);
    }

    private SeaTunnelRow filterFields(SeaTunnelRow row, String tableId) {
        if (selectAllMap.get(tableId)) {
            return row;
        }

        if (readColumnsMap == null || readColumnsMap.isEmpty()) {
            return row;
        }

        CatalogTable table = findTableByTableId(tableId);
        if (table == null) {
            log.warn("Table not found for tableId: {}", tableId);
            return row;
        }

        List<String> allFieldNames = table.getTableSchema().getColumnNames();
        List<Integer> selectedIndices = new ArrayList<>();

        List<String> readColumns = readColumnsMap.get(tableId);

        for (String col : readColumns) {
            int index = allFieldNames.indexOf(col);
            if (index >= 0) {
                selectedIndices.add(index);
            } else {
                log.warn("Selected field '{}' not found in table '{}'", col, tableId);
            }
        }

        if (selectedIndices.isEmpty()) {
            throw new IllegalArgumentException(
                    "No valid selected fields found for table：" + tableId);
        }

        SeaTunnelRow filteredRow = new SeaTunnelRow(selectedIndices.size());
        filteredRow.setRowKind(row.getRowKind());
        filteredRow.setTableId(row.getTableId());

        for (int i = 0; i < selectedIndices.size(); i++) {
            filteredRow.setField(i, row.getField(selectedIndices.get(i)));
        }

        return filteredRow;
    }

    private CatalogTable findTableByTableId(String tableId) {
        for (CatalogTable table : tables) {
            if (table.getTablePath().toString().equals(tableId)) {
                return table;
            }
        }
        return null;
    }

    @Override
    public List<CatalogTable> getProducedType() {
        return tables;
    }

    @Override
    public SchemaChangeResolver getSchemaChangeResolver() {
        return schemaChangeResolver;
    }

    private static Map<String, SeaTunnelRowDebeziumDeserializationConverters>
            createTableRowConverters(
                    List<CatalogTable> tables,
                    MetadataConverter[] metadataConverters,
                    ZoneId serverTimeZone,
                    DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters =
                new HashMap<>();
        if (tables.size() > 1) {
            for (CatalogTable table : tables) {
                SeaTunnelRowDebeziumDeserializationConverters itemRowConverter =
                        new SeaTunnelRowDebeziumDeserializationConverters(
                                table.getSeaTunnelRowType(),
                                metadataConverters,
                                serverTimeZone,
                                userDefinedConverterFactory);
                tableRowConverters.put(table.getTablePath().toString(), itemRowConverter);
            }
            return tableRowConverters;
        }

        SeaTunnelRowDebeziumDeserializationConverters tableRowConverter =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        tables.get(0).getSeaTunnelRowType(),
                        metadataConverters,
                        serverTimeZone,
                        userDefinedConverterFactory);
        tableRowConverters.put(DEFAULT_TABLE_NAME_KEY, tableRowConverter);
        return tableRowConverters;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private List<CatalogTable> tables;
        private MetadataConverter[] metadataConverters = new MetadataConverter[0];
        private ZoneId serverTimeZone = ZoneId.of("UTC");
        private DebeziumDeserializationConverterFactory userDefinedConverterFactory =
                DebeziumDeserializationConverterFactory.DEFAULT;
        private SchemaChangeResolver schemaChangeResolver;
        private Map<TableId, Struct> tableIdTableChangeMap;
        private Map<String, List<String>> readColumnsMap;;

        public SeaTunnelRowDebeziumDeserializeSchema build() {
            return new SeaTunnelRowDebeziumDeserializeSchema(
                    metadataConverters,
                    tables,
                    serverTimeZone,
                    userDefinedConverterFactory,
                    schemaChangeResolver,
                    tableIdTableChangeMap,
                    readColumnsMap);
        }
    }
}
