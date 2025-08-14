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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.source.offset.LsnOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;

import org.apache.kafka.connect.data.Struct;

import com.google.auto.service.AutoService;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class SqlServerIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism, SupportColumnProjection {

    static final String IDENTIFIER = "SqlServer-CDC";

    public SqlServerIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return SqlServerSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return SqlServerSourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        SqlServerSourceConfigFactory configFactory = new SqlServerSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        JdbcUrlUtil.UrlInfo urlInfo =
                SqlServerURLParser.parse(config.get(JdbcCatalogOptions.BASE_URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(
                urlInfo.getPort() == null
                        ? getPortNumberFromJdbcConnection(urlInfo, config)
                        : urlInfo.getPort());
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        Map<String, List<String>> readColumnsMap =
                JdbcSourceTableConfig.toReadColumnsMap(
                        config.get(JdbcSourceOptions.TABLE_NAMES_CONFIG));
        readColumnsMap =
                readColumnsMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().replace("[", "").replace("]", ""),
                                        Map.Entry::getValue,
                                        (v1, v2) -> v1,
                                        LinkedHashMap::new));
        Map<TableId, Struct> tableIdStructMap = tableChanges(readColumnsMap);
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES), tableIdStructMap);
        }

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);

        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setTableIdTableChangeMap(tableIdStructMap)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .setReadColumnsMap(readColumnsMap)
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new SqlServerDialect((SqlServerSourceConfigFactory) configFactory, catalogTables);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new LsnOffsetFactory(
                (SqlServerSourceConfigFactory) configFactory, (SqlServerDialect) dataSourceDialect);
    }

    private int getPortNumberFromJdbcConnection(
            JdbcUrlUtil.UrlInfo urlInfo, ReadonlyConfig config) {
        try (Connection connection =
                DriverManager.getConnection(
                        urlInfo.getOrigin(),
                        config.get(JdbcCatalogOptions.USERNAME),
                        config.get(JdbcCatalogOptions.PASSWORD))) {
            final Class<? extends Connection> aClass = connection.getClass();
            Field privateField = aClass.getDeclaredField("activeConnectionProperties");
            privateField.setAccessible(true);
            Properties fieldValue = (Properties) privateField.get(connection);
            String portNumber = (String) fieldValue.get("portNumber");
            return Integer.parseInt(portNumber);
        } catch (Exception e) {
            throw new SeaTunnelException("getPortNumberFromJdbcConnection error", e);
        }
    }

    private Map<TableId, Struct> tableChanges(Map<String, List<String>> readColumnsMap) {
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        SqlServerDialect dialect =
                new SqlServerDialect((SqlServerSourceConfigFactory) configFactory, catalogTables);
        List<TableId> discoverTables = dialect.discoverDataCollections(jdbcSourceConfig);
        ConnectTableChangeSerializer connectTableChangeSerializer =
                new ConnectTableChangeSerializer();
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(jdbcSourceConfig)) {
            return discoverTables.stream()
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    (tableId) -> {
                                        TableChanges tableChanges = new TableChanges();
                                        Table originalTable =
                                                dialect.queryTableSchema(jdbcConnection, tableId)
                                                        .getTable();

                                        Table filteredTable =
                                                filterTableColumns(
                                                        originalTable, tableId, readColumnsMap);

                                        tableChanges.create(filteredTable);
                                        return connectTableChangeSerializer
                                                .serialize(tableChanges)
                                                .get(0);
                                    }));
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    private Table filterTableColumns(
            Table originalTable, TableId tableId, Map<String, List<String>> readColumnsMap) {
        String tableKey = tableId.toString();
        List<String> readColumns = readColumnsMap.get(tableKey);

        if (readColumns == null || readColumns.isEmpty()) {
            return originalTable;
        }

        List<Column> filteredColumns =
                originalTable.columns().stream()
                        .filter(column -> readColumns.contains(column.name()))
                        .collect(Collectors.toList());

        List<String> filteredPrimaryKeyNames =
                originalTable.primaryKeyColumnNames().stream()
                        .filter(readColumns::contains)
                        .collect(Collectors.toList());

        return Table.editor()
                .tableId(tableId)
                .addColumns(filteredColumns)
                .setPrimaryKeyNames(filteredPrimaryKeyNames)
                .create();
    }

    @Override
    public Optional<String> driverName() {
        return Optional.of("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }
}
