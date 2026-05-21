/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;

/**
 * Description: createKingbaseDataEventsForTable into the reference entity
 *
 * @author czy
 * @date 2023/06/02
 */
public class KingbaseDataEventsParam {
    private final ChangeEventSource.ChangeEventSourceContext sourceContext;
    private final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                    KingbasePartition, KingbaseOffsetContext>
            snapshotContext;
    private final EventDispatcher.SnapshotReceiver<KingbasePartition> snapshotReceiver;
    private final Table table;
    private final boolean isLastTable;

    /**
     * Constructor
     *
     * @param sourceContext ChangeEventSource.ChangeEventSourceContext
     * @param snapshotContext
     *     RelationalSnapshotChangeEventSource.RelationalSnapshotContext<KingbasePartition,
     *     KingbaseOffsetContext>
     * @param snapshotReceiver EventDispatcher.SnapshotReceiver snapshotReceiver
     * @param table Table
     * @param isLastTable boolean
     */
    public KingbaseDataEventsParam(
            ChangeEventSource.ChangeEventSourceContext sourceContext,
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                            KingbasePartition, KingbaseOffsetContext>
                    snapshotContext,
            EventDispatcher.SnapshotReceiver<KingbasePartition> snapshotReceiver,
            Table table,
            boolean isLastTable) {
        this.sourceContext = sourceContext;
        this.snapshotContext = snapshotContext;
        this.snapshotReceiver = snapshotReceiver;
        this.table = table;
        this.isLastTable = isLastTable;
    }

    /**
     * Get ChangeEventSource ChangeEventSourceContext
     *
     * @return sourceContext
     */
    public ChangeEventSource.ChangeEventSourceContext getSourceContext() {
        return sourceContext;
    }

    /**
     * Get Snapshot Context
     *
     * @return snapshotContext RelationalSnapshotChangeEventSource.RelationalSnapshotContext
     */
    public RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                    KingbasePartition, KingbaseOffsetContext>
            getSnapshotContext() {
        return snapshotContext;
    }

    /**
     * Get Snapshot Receiver
     *
     * @return snapshotReceiver
     */
    public EventDispatcher.SnapshotReceiver<KingbasePartition> getSnapshotReceiver() {
        return snapshotReceiver;
    }

    /**
     * Get table
     *
     * @return table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get isLastTable
     *
     * @return isLastTable
     */
    public boolean isLastTable() {
        return isLastTable;
    }
}
