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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset.OracleAgentOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.apache.kafka.connect.data.Struct;

import com.google.auto.service.AutoService;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import lombok.NoArgsConstructor;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.DEBEZIUM_PROPERTIES;
import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class OracleAgentIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism, SupportColumnProjection {

    static final String IDENTIFIER = "OracleAgent-CDC";

    private OracleAgentSourceConfig sourceConfig;

    public OracleAgentIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OracleAgentSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return OracleAgentSourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        OracleAgentSourceConfigFactory configFactory =
                new OracleAgentSourceConfigFactory(
                        config.get(OracleAgentSourceOptions.ORACLE9BRIDGE_AGENT_HOST),
                        config.get(OracleAgentSourceOptions.ORACLE9BRIDGE_AGENT_PORT));
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        configFactory.originUrl(config.get(JdbcCatalogOptions.BASE_URL));
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        Map<TableId, Struct> tableIdStructMap = tableChanges();
        if (COMPATIBLE_DEBEZIUM_JSON.equals(config.get(FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(DEBEZIUM_PROPERTIES), tableIdStructMap);
        }

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);

        Map<String, List<String>> readColumnsMap =
                JdbcSourceTableConfig.toReadColumnsMap(
                        config.get(JdbcSourceOptions.TABLE_NAMES_CONFIG));

        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .setTableIdTableChangeMap(tableIdStructMap)
                        .setReadColumnsMap(readColumnsMap)
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OracleAgentDialect(getSourceConfig(), catalogTables);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new OracleAgentOffsetFactory(
                getSourceConfig(), (OracleAgentDialect) dataSourceDialect);
    }

    @Override
    public Optional<String> driverName() {
        return Optional.of("oracle.jdbc.OracleDriver");
    }

    private synchronized OracleAgentSourceConfig getSourceConfig() {
        if (sourceConfig != null) {
            return sourceConfig;
        }
        sourceConfig = (OracleAgentSourceConfig) configFactory.create(0);
        return sourceConfig;
    }

    private Map<TableId, Struct> tableChanges() {
        OracleAgentSourceConfig jdbcSourceConfig =
                (OracleAgentSourceConfig) configFactory.create(0);
        OracleAgentDialect dialect = new OracleAgentDialect(jdbcSourceConfig, catalogTables);
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
                                        tableChanges.create(
                                                dialect.queryTableSchema(jdbcConnection, tableId)
                                                        .getTable());
                                        return connectTableChangeSerializer
                                                .serialize(tableChanges)
                                                .get(0);
                                    }));
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }
}
