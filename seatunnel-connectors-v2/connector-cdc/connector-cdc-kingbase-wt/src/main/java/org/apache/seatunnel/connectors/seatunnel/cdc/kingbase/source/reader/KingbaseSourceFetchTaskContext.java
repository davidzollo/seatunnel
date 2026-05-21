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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.config.KingbaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.KingbaseUtils;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.KingbaseErrorHandler;
import io.debezium.connector.kingbase.KingbaseEventDispatcher;
import io.debezium.connector.kingbase.KingbaseEventMetadataProvider;
import io.debezium.connector.kingbase.KingbaseOffsetContext;
import io.debezium.connector.kingbase.KingbasePartition;
import io.debezium.connector.kingbase.KingbaseSchema;
import io.debezium.connector.kingbase.KingbaseTaskContext;
import io.debezium.connector.kingbase.KingbaseTopicSelector;
import io.debezium.connector.kingbase.TypeRegistry;
import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.connector.kingbase.connection.ReplicationConnection;
import io.debezium.connector.kingbase.spi.SlotState;
import io.debezium.connector.kingbase.spi.Snapshotter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;

import static org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.KingbaseConnectionUtils.newKingbaseValueConverterBuilder;

@Slf4j
public class KingbaseSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final String CONTEXT_NAME = "kingbase-cdc-connector-task";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KingbaseSourceFetchTaskContext.class);

    private final KingbaseConnection dataConnection;

    @Getter private ReplicationConnection replicationConnection;

    private final KingbaseEventMetadataProvider metadataProvider;

    @Getter private Snapshotter snapshotter;
    private KingbaseSchema databaseSchema;
    private KingbaseOffsetContext offsetContext;
    private KingbasePartition partition;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<KingbasePartition> dispatcher;
    private KingbaseEventDispatcher<TableId> kingbaseEventDispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private KingbaseErrorHandler errorHandler;

    @Getter private KingbaseTaskContext taskContext;

    private SnapshotChangeEventSourceMetrics<KingbasePartition> snapshotChangeEventSourceMetrics;

    private KingbaseConnection.KingbaseValueConverterBuilder kingbaseValueConverterBuilder;

    private Collection<TableChanges.TableChange> engineHistory;

    public KingbaseSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            KingbaseConnection dataConnection,
            Collection<TableChanges.TableChange> engineHistory) {
        super(sourceConfig, dataSourceDialect);
        this.dataConnection = dataConnection;
        this.metadataProvider = new KingbaseEventMetadataProvider();
        this.engineHistory = engineHistory;
        this.kingbaseValueConverterBuilder =
                newKingbaseValueConverterBuilder(getDbzConnectorConfig());
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        super.registerDatabaseHistory(sourceSplitBase, dataConnection);

        // initial stateful objects
        final KingbaseConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.snapshotter = connectorConfig.getSnapshotter();

        this.topicSelector = KingbaseTopicSelector.create(connectorConfig);

        final TypeRegistry typeRegistry = dataConnection.getTypeRegistry();

        this.databaseSchema =
                new KingbaseSchema(
                        connectorConfig,
                        typeRegistry,
                        topicSelector,
                        kingbaseValueConverterBuilder.build(typeRegistry));
        this.taskContext = new KingbaseTaskContext(connectorConfig, databaseSchema, topicSelector);
        try {
            taskContext.refreshSchema(dataConnection, false);
        } catch (SQLException e) {
            throw new DebeziumException("load schema failed", e);
        }
        this.offsetContext =
                loadStartingOffsetState(
                        new KingbaseOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.partition = new KingbasePartition(connectorConfig.getLogicalName());

        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();

        LoggingContext.PreviousContext previousContext =
                taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(dataConnection.serverInfo().toString());
                }
                slotInfo =
                        dataConnection.getReplicationSlotState(
                                connectorConfig.slotName(),
                                connectorConfig.plugin().getPostgresPluginName());
            } catch (SQLException e) {
                LOGGER.warn(
                        "unable to load info of replication slot, Debezium will try to create the slot");
            }

            if (offsetContext == null) {
                LOGGER.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            } else {
                LOGGER.info("Found previous offset {}", offsetContext);
                snapshotter.init(connectorConfig, offsetContext.asOffsetState(), slotInfo);
            }

            if (snapshotter.shouldStream()) {
                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking
                // place
                if (replicationConnection == null) {
                    this.replicationConnection =
                            createReplicationConnection(
                                    this.taskContext,
                                    snapshotter.shouldSnapshot(),
                                    connectorConfig.maxRetries(),
                                    connectorConfig.retryDelay());
                    try {
                        // create the slot if it doesn't exist, otherwise update slot to add new
                        // table(job restore and add table)
                        replicationConnection.createReplicationSlot().orElse(null);
                    } catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message +=
                                    "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                            LOGGER.warn(message);
                        } else {
                            throw new DebeziumException(message, ex);
                        }
                    }
                }
            }

            try {
                dataConnection.commit();
            } catch (SQLException e) {
                throw new DebeziumException(e);
            }

            this.queue =
                    new ChangeEventQueue.Builder<DataChangeEvent>()
                            .pollInterval(connectorConfig.getPollInterval())
                            .maxBatchSize(connectorConfig.getMaxBatchSize())
                            .maxQueueSize(queueSize)
                            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                            .loggingContextSupplier(
                                    () -> taskContext.configureLoggingContext(CONTEXT_NAME))
                            // do not buffer any element, we use signal event
                            // .buffering()
                            .build();

            this.dispatcher =
                    new JdbcSourceEventDispatcher<>(
                            connectorConfig,
                            topicSelector,
                            databaseSchema,
                            queue,
                            connectorConfig.getTableFilters().dataCollectionFilter(),
                            DataChangeEvent::new,
                            metadataProvider,
                            schemaNameAdjuster);

            this.kingbaseEventDispatcher =
                    new KingbaseEventDispatcher<>(
                            connectorConfig,
                            topicSelector,
                            databaseSchema,
                            queue,
                            connectorConfig.getTableFilters().dataCollectionFilter(),
                            DataChangeEvent::new,
                            metadataProvider,
                            schemaNameAdjuster);

            this.snapshotChangeEventSourceMetrics =
                    new DefaultChangeEventSourceMetricsFactory<KingbasePartition>()
                            .getSnapshotMetrics(taskContext, queue, metadataProvider);

            this.errorHandler = new KingbaseErrorHandler(connectorConfig, queue);
        } finally {
            previousContext.restore();
        }
    }

    @Override
    public KingbaseSourceConfig getSourceConfig() {
        return (KingbaseSourceConfig) sourceConfig;
    }

    public KingbaseConnection getDataConnection() {
        return dataConnection;
    }

    public ReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    public Snapshotter getSnapshotter() {
        return snapshotter;
    }

    public KingbaseTaskContext getTaskContext() {
        return taskContext;
    }

    public SnapshotChangeEventSourceMetrics<KingbasePartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    @Override
    public KingbaseConnectorConfig getDbzConnectorConfig() {
        return (KingbaseConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public KingbaseOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public KingbasePartition getPartition() {
        return partition;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public KingbaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return KingbaseUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher<KingbasePartition> getDispatcher() {
        return dispatcher;
    }

    public KingbaseEventDispatcher<TableId> getKingbaseEventDispatcher() {
        return kingbaseEventDispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return KingbaseUtils.getLsnPosition(sourceRecord);
    }

    @Override
    public void close() {
        try {
            this.dataConnection.close();
            if (this.replicationConnection != null) {
                this.replicationConnection.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to close connection", e);
        }
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private KingbaseOffsetContext loadStartingOffsetState(
            KingbaseOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        return loader.load(offset.getOffset());
    }

    public ReplicationConnection createReplicationConnection(
            KingbaseTaskContext taskContext,
            boolean doSnapshot,
            int maxRetries,
            Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(doSnapshot);
            } catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error(
                            "Too many errors connecting to server. All {} retries failed.",
                            maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn(
                        "Error connecting to server; will attempt retry {} of {} after {} "
                                + "seconds. Exception message: {}",
                        retryCount,
                        maxRetries,
                        retryDelay.getSeconds(),
                        ex.getMessage());
                try {
                    metronome.pause();
                } catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }
}
