/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbase;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;

import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.kingbase.connection.LogicalDecodingMessage;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Custom extension of the {@link EventDispatcher} to accommodate routing {@link
 * LogicalDecodingMessage} events to the change event queue.
 *
 * @author Lairen Hightower
 */
public class KingbaseEventDispatcher<T extends DataCollectionId>
        extends JdbcSourceEventDispatcher<KingbasePartition> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseEventDispatcher.class);
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final LogicalDecodingMessageMonitor logicalDecodingMessageMonitor;
    private final LogicalDecodingMessageFilter messageFilter;

    public KingbaseEventDispatcher(
            KingbaseConnectorConfig connectorConfig,
            TopicSelector<TableId> topicSelector,
            DatabaseSchema<TableId> schema,
            ChangeEventQueue<DataChangeEvent> queue,
            DataCollectionFilters.DataCollectionFilter<TableId> filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            SchemaNameAdjuster schemaNameAdjuster) {
        this(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                null,
                metadataProvider,
                null,
                schemaNameAdjuster,
                null);
    }

    public KingbaseEventDispatcher(
            KingbaseConnectorConfig connectorConfig,
            TopicSelector<TableId> topicSelector,
            DatabaseSchema<TableId> schema,
            ChangeEventQueue<DataChangeEvent> queue,
            DataCollectionFilters.DataCollectionFilter<TableId> filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            Heartbeat heartbeat,
            SchemaNameAdjuster schemaNameAdjuster) {
        this(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                null,
                metadataProvider,
                heartbeat,
                schemaNameAdjuster,
                null);
    }

    public KingbaseEventDispatcher(
            KingbaseConnectorConfig connectorConfig,
            TopicSelector<TableId> topicSelector,
            DatabaseSchema<TableId> schema,
            ChangeEventQueue<DataChangeEvent> queue,
            DataCollectionFilters.DataCollectionFilter<TableId> filter,
            ChangeEventCreator changeEventCreator,
            InconsistentSchemaHandler<KingbasePartition, T> inconsistentSchemaHandler,
            EventMetadataProvider metadataProvider,
            Heartbeat customHeartbeat,
            SchemaNameAdjuster schemaNameAdjuster,
            JdbcConnection jdbcConnection) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                schemaNameAdjuster);
        this.queue = queue;
        this.logicalDecodingMessageMonitor =
                new LogicalDecodingMessageMonitor(
                        connectorConfig, this::enqueueLogicalDecodingMessage);
        this.messageFilter = connectorConfig.getMessageFilter();
    }

    public void dispatchLogicalDecodingMessage(
            OffsetContext offset, Long decodeTimestamp, LogicalDecodingMessage message)
            throws InterruptedException {
        if (messageFilter.isIncluded(message.getPrefix())) {
            logicalDecodingMessageMonitor.logicalDecodingMessageEvent(
                    offset, decodeTimestamp, message);
        } else {
            LOGGER.trace(
                    "Filtered data change event for logical decoding message with prefix{}",
                    message.getPrefix());
        }
    }

    private void enqueueLogicalDecodingMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }
}
