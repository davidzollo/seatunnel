/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbase;

import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.connector.kingbase.connection.ReplicationMessage;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Description: Message to clear table data
 *
 * @author czy
 * @date 2023/06/02
 */
public class TruncateRecordEmitter extends KingbaseChangeRecordEmitter {
    /**
     * Constructor
     *
     * @param partition Partition
     * @param offset OffsetContext
     * @param clock Clock
     * @param connectorConfig KingbaseConnectorConfig
     * @param schema KingbaseSchema
     * @param connection KingbaseConnection
     * @param tableId TableId
     * @param message ReplicationMessage
     */
    public TruncateRecordEmitter(
            KingbasePartition partition,
            OffsetContext offset,
            Clock clock,
            KingbaseConnectorConfig connectorConfig,
            KingbaseSchema schema,
            KingbaseConnection connection,
            TableId tableId,
            ReplicationMessage message) {
        super(partition, offset, clock, connectorConfig, schema, connection, tableId, message);
    }
}
