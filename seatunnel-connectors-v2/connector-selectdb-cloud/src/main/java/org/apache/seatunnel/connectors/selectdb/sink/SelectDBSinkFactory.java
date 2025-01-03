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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkState;
import org.apache.seatunnel.connectors.selectdb.util.UnsupportedTypeConverterUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_TTL_SEC;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.BASE_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.CLUSTER_NAME;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.DATABASE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.LOAD_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SELECTDB_SINK_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_BUFFER_COUNT;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_ENABLE_2PC;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_ENABLE_DELETE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_FLUSH_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_LABEL_PREFIX;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_MAX_RETRIES;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.TABLE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.USERNAME;

@AutoService(Factory.class)
public class SelectDBSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return "SelectDBCloud";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BASE_URL, LOAD_URL, CLUSTER_NAME, USERNAME, PASSWORD)
                .optional(
                        DATABASE,
                        TABLE,
                        SINK_MAX_RETRIES,
                        SINK_LABEL_PREFIX,
                        SINK_BUFFER_SIZE,
                        SINK_ENABLE_2PC,
                        SINK_BUFFER_COUNT,
                        SINK_ENABLE_DELETE,
                        SINK_FLUSH_QUEUE_SIZE,
                        DATA_SAVE_MODE,
                        SCHEMA_SAVE_MODE,
                        SAVE_MODE_CREATE_TEMPLATE,
                        SELECTDB_SINK_CONFIG_PREFIX,
                        MULTI_TABLE_SINK_REPLICA,
                        MULTI_TABLE_SINK_TTL_SEC,
                        NEEDS_UNSUPPORTED_TYPE_CASTING)
                .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Arrays.asList(SAVE_MODE_CREATE_TEMPLATE.key());
    }

    @Override
    public TableSink<SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        CatalogTable catalogTable =
                options.get(NEEDS_UNSUPPORTED_TYPE_CASTING)
                        ? UnsupportedTypeConverterUtils.convertCatalogTable(
                                context.getCatalogTable())
                        : context.getCatalogTable();
        return () -> new SelectDBSink(renameCatalogTable(options, catalogTable), options);
    }

    private CatalogTable renameCatalogTable(ReadonlyConfig options, CatalogTable catalogTable) {

        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(options.get(TABLE))) {
            tableName = options.get(TABLE);
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(options.get(DATABASE))) {
            namespace = options.get(DATABASE);
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(tableId.getCatalogName(), namespace, null, tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }

    @VisibleForTesting
    CatalogTable replaceColumnName(CatalogTable catalogTable, String original, String replacement) {
        checkNotNull(original, "original can not be null");
        checkNotNull(replacement, "replacement can not be null");
        checkArgument(StringUtils.isNotEmpty(original), "original can not be empty");
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        Map<String, String> changedName = new LinkedHashMap<>();
        List<Column> newColumns =
                columns.stream()
                        .map(
                                column -> {
                                    if (column.getName().contains(original)) {
                                        changedName.put(
                                                column.getName(),
                                                column.getName().replace(original, replacement));
                                        return column.rename(changedName.get(column.getName()));
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());
        List<String> newPrimaryKey = null;
        if (catalogTable.getTableSchema().getPrimaryKey() != null
                && catalogTable.getTableSchema().getPrimaryKey().getColumnNames() != null) {
            newPrimaryKey =
                    catalogTable.getTableSchema().getPrimaryKey().getColumnNames().stream()
                            .map(key -> changedName.getOrDefault(key, key))
                            .collect(Collectors.toList());
        }
        List<ConstraintKey> newConstraintKeys =
                catalogTable.getTableSchema().getConstraintKeys().stream()
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
                                                catalogTable
                                                        .getTableSchema()
                                                        .getPrimaryKey()
                                                        .getPrimaryKey(),
                                                newPrimaryKey))
                        .constraintKey(newConstraintKeys)
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                newTableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment());
    }
}
