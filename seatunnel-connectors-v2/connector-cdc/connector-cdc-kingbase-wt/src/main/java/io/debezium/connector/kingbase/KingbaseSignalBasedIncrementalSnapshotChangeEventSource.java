/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * Custom Kingbase implementation of the {@link SignalBasedIncrementalSnapshotChangeEventSource}
 * implementation which performs an explicit schema refresh of a table prior to the incremental
 * snapshot starting.
 *
 * @author Chris Cranford
 */
public class KingbaseSignalBasedIncrementalSnapshotChangeEventSource
        extends SignalBasedIncrementalSnapshotChangeEventSource<KingbasePartition, TableId> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(KingbaseSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final KingbaseConnection jdbcConnection;
    private final KingbaseSchema schema;

    public KingbaseSignalBasedIncrementalSnapshotChangeEventSource(
            RelationalDatabaseConnectorConfig config,
            JdbcConnection jdbcConnection,
            EventDispatcher<KingbasePartition, TableId> dispatcher,
            DatabaseSchema<?> databaseSchema,
            Clock clock,
            SnapshotProgressListener<KingbasePartition> progressListener,
            DataChangeEventListener<KingbasePartition> dataChangeEventListener) {
        super(
                config,
                jdbcConnection,
                dispatcher,
                databaseSchema,
                clock,
                progressListener,
                dataChangeEventListener);
        this.jdbcConnection = (KingbaseConnection) jdbcConnection;
        this.schema = (KingbaseSchema) databaseSchema;
    }
}
