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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.base.utils.WhereConditionClauseHook;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.KingbaseUtils;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.KingbaseOffsetContext;
import io.debezium.connector.kingbase.KingbasePartition;
import io.debezium.connector.kingbase.KingbaseSchema;
import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;

@Slf4j
public class KingbaseSnapshotSplitReadTask
        extends AbstractSnapshotChangeEventSource<KingbasePartition, KingbaseOffsetContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KingbaseSnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final KingbaseConnectorConfig connectorConfig;
    private final KingbaseSchema databaseSchema;
    private final KingbaseConnection jdbcConnection;
    private final JdbcSourceEventDispatcher<KingbasePartition> dispatcher;
    private final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final KingbaseOffsetContext offsetContext;
    private final SnapshotProgressListener<KingbasePartition> snapshotProgressListener;

    public KingbaseSnapshotSplitReadTask(
            KingbaseConnectorConfig connectorConfig,
            KingbaseOffsetContext previousOffset,
            SnapshotProgressListener<KingbasePartition> snapshotProgressListener,
            KingbaseSchema databaseSchema,
            KingbaseConnection jdbcConnection,
            JdbcSourceEventDispatcher<KingbasePartition> dispatcher,
            SnapshotSplit snapshotSplit) {
        super(connectorConfig, snapshotProgressListener);
        this.offsetContext = previousOffset;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = Clock.SYSTEM;
        this.snapshotSplit = snapshotSplit;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult<KingbaseOffsetContext> execute(
            ChangeEventSourceContext context,
            KingbasePartition partition,
            KingbaseOffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        final SnapshotContext<KingbasePartition, KingbaseOffsetContext> ctx;
        try {
            ctx = prepare(partition);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        try {
            return doExecute(context, previousOffset, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            throw new DebeziumException(t);
        }
    }

    @Override
    protected SnapshotResult<KingbaseOffsetContext> doExecute(
            ChangeEventSourceContext context,
            KingbaseOffsetContext previousOffset,
            SnapshotContext snapshotContext,
            SnapshottingTask snapshottingTask)
            throws Exception {
        final KingbaseSnapshotContext ctx = (KingbaseSnapshotContext) snapshotContext;
        ctx.offset = offsetContext;

        final LsnOffset lowWatermark = KingbaseUtils.currentLsn(jdbcConnection);
        LOGGER.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setLowWatermark(lowWatermark);
        dispatcher.dispatchWatermarkEvent(
                ctx.partition.getSourcePartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

        LOGGER.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        final LsnOffset highWatermark = KingbaseUtils.currentLsn(jdbcConnection);
        LOGGER.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setHighWatermark(highWatermark);
        dispatcher.dispatchWatermarkEvent(
                ctx.partition.getSourcePartition(),
                snapshotSplit,
                highWatermark,
                WatermarkKind.HIGH);
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(
            KingbasePartition partition, KingbaseOffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected SnapshotContext<KingbasePartition, KingbaseOffsetContext> prepare(
            KingbasePartition partition) throws Exception {
        return new KingbaseSnapshotContext(partition);
    }

    private void createDataEvents(KingbaseSnapshotContext snapshotContext, TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver<KingbasePartition> snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        LOGGER.debug("Snapshotting table {}", tableId);
        // todo pg 的 schema 不包含database
        TableId newTableId = new TableId(null, tableId.schema(), tableId.table());
        createDataEventsForTable(
                snapshotContext, snapshotReceiver, databaseSchema.tableFor(newTableId));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
            KingbaseSnapshotContext snapshotContext,
            EventDispatcher.SnapshotReceiver<KingbasePartition> snapshotReceiver,
            Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info(
                "Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                KingbaseUtils.buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null,
                        snapshotSplit.getSplitEnd(),
                        snapshotSplit.isNull(),
                        new WhereConditionClauseHook(
                                snapshotSplit.getWhereConditionClause(),
                                snapshotSplit.getReadColumnsMap().get(snapshotSplit.getTableId())));
        LOGGER.info(
                "For split '{}' of table {} using select statement: '{}'",
                snapshotSplit.splitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                        KingbaseUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType(),
                                connectorConfig.getQueryFetchSize(),
                                snapshotSplit.isNull());
                ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = columnArray.getColumns()[i];
                    row[columnArray.getColumns()[i].position() - 1] = readField(rs, i + 1);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(
                            snapshotContext.partition, table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                        snapshotContext.partition,
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            LOGGER.info(
                    "Finished exporting {} records for split '{}', total duration '{}'",
                    rows,
                    snapshotSplit.splitId(),
                    Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    protected ChangeRecordEmitter<KingbasePartition> getChangeRecordEmitter(
            KingbaseSnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter<>(
                snapshotContext.partition, snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private Object readField(ResultSet rs, int columnIndex) throws SQLException {
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnType = metaData.getColumnType(columnIndex);

        if (columnType == Types.TIME) {
            return rs.getTimestamp(columnIndex);
        } else {
            return rs.getObject(columnIndex);
        }
    }

    private static class KingbaseSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                    KingbasePartition, KingbaseOffsetContext> {

        public KingbaseSnapshotContext(KingbasePartition partition) throws SQLException {
            super(partition, "");
        }
    }
}
