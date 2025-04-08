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

package io.debezium.connector.oracle;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.DateUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleAgentClientUtils;

import org.apache.commons.collections4.CollectionUtils;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClient;
import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClientFactory;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleDDLOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleTransactionData;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleTransactionFileNumberFetchRequest;

import io.debezium.connector.oracle.oracleAgent.OracleAgentDmlEntry;
import io.debezium.connector.oracle.oracleAgent.OracleAgentDmlEntryFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class OracleAgentStreamingChangeEventSource
        implements StreamingChangeEventSource<OracleAgentOffsetContext> {

    private static final Long NO_DATA_AVAILABLE_SLEEP_MS = 5_000L;

    private final OracleAgentConnectorConfig oracle9BridgeConnectorConfig;
    private final CustomOracleAgentValueConverter customOracleAgentValueConverter;
    private final OracleAgentSourceConfig sourceConfig;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private ChangeEventSourceContext context;
    private final OracleDatabaseSchema oracleDatabaseSchema;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleAgentDmlEntryFactory dmlEntryFactory;
    private final ZoneId serverTimeZone;
    // todo: we don't support multiple database now, since the oracle9bridge event doesn't contains
    // the database field,
    // one oracle9bridge should only bind to one database instance.
    protected final Map<String, TableId> tableNameToIdMap;
    protected final List<String> tables;
    protected final List<String> tableOwners;

    public OracleAgentStreamingChangeEventSource(
            OracleAgentOffsetContext offsetContext,
            OracleAgentConnectorConfig connectorConfig,
            OracleConnection oracleConnection,
            List<TableId> tableIds,
            OracleAgentSourceConfig sourceConfig,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            OracleDatabaseSchema oracleDatabaseSchema,
            String serverTimeZone) {
        this.customOracleAgentValueConverter =
                new CustomOracleAgentValueConverter(connectorConfig, oracleConnection);
        this.oracle9BridgeConnectorConfig = connectorConfig;
        this.sourceConfig = sourceConfig;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.clock = Clock.system();
        this.oracleDatabaseSchema = oracleDatabaseSchema;
        tableNameToIdMap =
                tableIds.stream().collect(Collectors.toMap(TableId::table, Function.identity()));
        this.tables = tableIds.stream().map(TableId::table).collect(Collectors.toList());
        this.tableOwners = tableIds.stream().map(TableId::schema).collect(Collectors.toList());
        this.dmlEntryFactory = new OracleAgentDmlEntryFactory(ZoneId.of(serverTimeZone));
        this.serverTimeZone = ZoneId.of(serverTimeZone);
    }

    @Override
    public void execute(ChangeEventSourceContext context, OracleAgentOffsetContext offsetContext) {
        this.context = context;
        try {
            log.info(
                    "[{}] Start {} from fzsFileNumber={}, scn={}",
                    tables,
                    getClass().getName(),
                    offsetContext.getFzsFileNumber(),
                    offsetContext.getScn());
            long pollInterval = sourceConfig.getDbzConnectorConfig().getPollInterval().toMillis();
            OracleAgentClient oracle9BridgeClient =
                    OracleAgentClientFactory.getOrCreateStartedSocketClient(
                            sourceConfig.getOracleAgentHost(), sourceConfig.getOracleAgentPort());
            Integer currentFzsFileNumber = offsetContext.getFzsFileNumber();
            log.info("The current fzs file number is: {}", currentFzsFileNumber);
            if (offsetContext.getFzsFileNumber() == 0) {
                currentFzsFileNumber =
                        OracleAgentClientUtils.currentMinFzsFileNumber(
                                oracle9BridgeClient,
                                new OracleTransactionFileNumberFetchRequest(tableOwners, tables));
                log.info(
                        "The fzs file number from offset is 0, fetched the currentFzsFileNumber: {} from agent",
                        currentFzsFileNumber);
            }

            while (context.isRunning()) {
                List<OracleTransactionData> oracleTransactionData =
                        OracleAgentClientUtils.fetchOracleTransactionData(
                                oracle9BridgeClient, tableOwners, tables, currentFzsFileNumber);
                if (CollectionUtils.isEmpty(oracleTransactionData)) {
                    log.debug(
                            "There is no data for tables: {} in the current fzs file: {}",
                            tables,
                            currentFzsFileNumber);
                    Integer maxFzsFileNumber =
                            OracleAgentClientUtils.currentMaxFzsFileNumber(
                                    oracle9BridgeClient, tableOwners, tables);
                    if (currentFzsFileNumber < maxFzsFileNumber) {
                        log.info("The fzs file: {} is broken will skip it", currentFzsFileNumber);
                        currentFzsFileNumber++;
                    } else {
                        log.info(
                                "[{}] There is no data in the related fzs files: {}",
                                tables,
                                currentFzsFileNumber);
                        Thread.sleep(NO_DATA_AVAILABLE_SLEEP_MS);
                    }
                    eventDispatcher.dispatchHeartbeatEvent(offsetContext);
                    continue;
                }
                for (OracleTransactionData data : oracleTransactionData) {
                    // todo: filter the already processed scn in snapshot stage
                    handleEvent(offsetContext, currentFzsFileNumber, data.getOp());
                }
                Scn preScn = offsetContext.getScn();
                if (offsetContext.getScn().compareTo(preScn) != 1) {
                    eventDispatcher.dispatchHeartbeatEvent(offsetContext);
                } else {
                    log.info("[{}] The scn is invalided, will skip the event: {}", tables, preScn);
                }
                log.info(
                        "[{}] Success fetch data: {} from the OracleAgent: {} file, increase fzs file number to: {}",
                        tables,
                        oracleTransactionData.size(),
                        currentFzsFileNumber,
                        currentFzsFileNumber + 1);
                currentFzsFileNumber++;
                Thread.sleep(pollInterval);
            }
        } catch (Exception e) {
            log.error("[{}] Fzs fetch task stopped", tables, e);
            errorHandler.setProducerThrowable(e);
        }
    }

    protected void handleEvent(
            OracleAgentOffsetContext offsetContext,
            Integer fzsFileNumber,
            List<OracleOperation> oracleOperations) {
        for (OracleOperation oracleOperation : oracleOperations) {
            TableId tableId = tableNameToIdMap.get(oracleOperation.getTable());
            Table table = oracleDatabaseSchema.tableFor(tableId);
            if (table == null) {
                throw new IllegalArgumentException(
                        "The table: "
                                + tableId
                                + " is not found in the schema, exist table is: "
                                + oracleDatabaseSchema.getTables());
            }

            Scn scn = new Scn(new BigInteger(oracleOperation.getScn(), 16));
            if (OracleDDLOperation.TYPE.equals(oracleOperation.getType())) {
                log.info(
                        "The DDL: {} of the OracleAgent-CDC connector is not supported, will skip it",
                        oracleOperation);
                offsetContext.event(
                        tableId, DateUtils.toInstant(oracleOperation.getScntime(), serverTimeZone));
                offsetContext.setScn(scn);
                offsetContext.setFzsFileNumber(fzsFileNumber);
                continue;
            }
            List<OracleAgentDmlEntry> dmlEntries =
                    dmlEntryFactory.transformOperation(
                            customOracleAgentValueConverter, oracleOperation, table);
            for (OracleAgentDmlEntry dmlEntry : dmlEntries) {
                offsetContext.event(
                        tableId, DateUtils.toInstant(oracleOperation.getScntime(), serverTimeZone));
                offsetContext.setScn(scn);
                offsetContext.setFzsFileNumber(fzsFileNumber);
                try {
                    eventDispatcher.dispatchDataChangeEvent(
                            tableId,
                            new OracleDataChangeRecordEmitter(offsetContext, clock, dmlEntry));
                } catch (InterruptedException e) {
                    throw new RuntimeException("Dispatch DataChange Event Error", e);
                }
            }
        }
    }
}
