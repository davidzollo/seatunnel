/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import org.apache.kafka.connect.data.Struct;

import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.connector.kingbase.connection.Lsn;
import io.debezium.connector.kingbase.connection.ReplicationMessage;
import io.debezium.connector.kingbase.spi.SlotCreationResult;
import io.debezium.connector.kingbase.spi.SlotState;
import io.debezium.connector.kingbase.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Description: Kingbase database is changed as a remote snapshot
 *
 * @author czy
 * @since 2023-06-07
 */
public class KingbaseSnapshotChangeEventSource
        extends RelationalSnapshotChangeEventSource<KingbaseOffsetContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KingbaseSnapshotChangeEventSource.class);
    private static final String DELIMITER = " | ";
    private static final int MEMORY_UNIT = 1024;
    private static final String METADATASQL =
            "select"
                    + "    c.relname tableName,"
                    + "    c.reltuples tableRows,"
                    + "    case"
                    + "        when c.reltuples > 0 then pg_table_size(c.oid) / c.reltuples"
                    + "        else 0"
                    + "    end as avgRowLength "
                    + " from"
                    + "    pg_class c"
                    + "    LEFT JOIN pg_namespace n on n.oid = c.relnamespace"
                    + " where"
                    + "    n.nspname = '%s' "
                    + "    and c.relname = '%s' "
                    + " order by"
                    + "    c.reltuples asc;";

    private final KingbaseConnectorConfig connectorConfig;
    private final KingbaseConnection jdbcConnection;
    private final KingbaseSchema schema;
    private final Snapshotter snapshotter;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final Object messLock = new Object();
    private final Object dirLock = new Object();
    private String csvPath;
    private BigInteger csvDirSize;
    private BigInteger pageSize = BigInteger.valueOf(2 * MEMORY_UNIT * MEMORY_UNIT);
    private AtomicInteger unlockCount = new AtomicInteger(0);

    public KingbaseSnapshotChangeEventSource(
            KingbaseConnectorConfig connectorConfig,
            Snapshotter snapshotter,
            KingbaseConnection jdbcConnection,
            KingbaseSchema schema,
            EventDispatcher<TableId> dispatcher,
            Clock clock,
            SnapshotProgressListener snapshotProgressListener,
            SlotCreationResult slotCreatedInfo,
            SlotState startingSlotInfo) {
        super(connectorConfig, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.snapshotter = snapshotter;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.csvPath = connectorConfig.getExportCsvPath();
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(KingbaseOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        } else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<KingbaseOffsetContext> prepare(
            ChangeEventSourceContext changeEventSourceContext) throws Exception {
        return new PostgresSnapshotContext(connectorConfig.databaseName());
    }

    @Override
    protected void connectionCreated(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext) throws Exception {
        // If using catch up streaming, the connector opens the transaction that the snapshot will
        // eventually use
        // before the catch up streaming starts. By looking at the current wal location, the
        // transaction can determine
        // where the catch up streaming should stop. The transaction is held open throughout the
        // catch up
        // streaming phase so that the snapshot is performed from a consistent view of the data.
        // Since the isolation
        // level on the transaction used in catch up streaming has already set the isolation level
        // and executed
        // statements, the transaction does not need to get set the level again here.
        if (snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
            setSnapshotTransactionIsolationLevel();
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<KingbaseOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"});
    }

    @Override
    protected void lockTablesForSchemaSnapshot(
            ChangeEventSourceContext sourceContext,
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Optional<String> lockStatement =
                snapshotter.snapshotTableLockingStatement(
                        lockTimeout, snapshotContext.capturedTables);

        if (lockStatement.isPresent()) {
            LOGGER.info(
                    "Waiting a maximum of '{}' seconds for each table lock",
                    lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(lockStatement.get());
            // now that we have the locks, refresh the schema
            schema.refresh(jdbcConnection, false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext) throws SQLException {}

    @Override
    protected void releaseDataSnapshotLocks(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext) throws Exception {
        jdbcConnection.executeWithoutCommitting("COMMIT;");
    }

    @Override
    protected void determineSnapshotOffset(
            RelationalSnapshotContext<KingbaseOffsetContext> ctx,
            KingbaseOffsetContext previousOffset)
            throws Exception {
        KingbaseOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
                // The connect framework, not the connector, manages triggering committing offset
                // state so the
                // replication stream may not have flushed the latest offset state during catch up
                // streaming.
                // The previousOffset variable is shared between the catch up streaming and snapshot
                // phases and
                // has the latest known offset state.
                offset =
                        KingbaseOffsetContext.initialContext(
                                connectorConfig,
                                jdbcConnection,
                                getClock(),
                                previousOffset.lastCommitLsn(),
                                previousOffset.lastCompletelyProcessedLsn());
            } else {
                offset =
                        KingbaseOffsetContext.initialContext(
                                connectorConfig, jdbcConnection, getClock());
            }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(KingbaseOffsetContext offset) throws SQLException {
        final Lsn xlogStart = getTransactionStartLsn();
        final long txId = jdbcConnection.currentTransactionId().longValue();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(
                xlogStart,
                offset.lastCompletelyProcessedLsn(),
                clock.currentTime(),
                txId,
                offset.xmin(),
                null);
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(KingbaseOffsetContext offset)
            throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(Lsn.valueOf(jdbcConnection.currentXLogLocation()));
    }

    private Lsn getTransactionStartLsn() throws SQLException {
        if (slotCreatedInfo != null) {
            // When performing an exported snapshot based on a newly created replication slot, the
            // txLogStart position
            // should be based on the replication slot snapshot transaction point. This is crucial
            // so that if any
            // SQL operations occur mid-snapshot that they'll be properly captured when streaming
            // begins; otherwise
            // they'll be lost.
            return slotCreatedInfo.startLsn();
        } else if (!snapshotter.shouldStreamEventsStartingFromSnapshot()
                && startingSlotInfo != null) {
            // Allow streaming to resume from where streaming stopped last rather than where the
            // current snapshot starts.
            SlotState currentSlotState =
                    jdbcConnection.getReplicationSlotState(
                            connectorConfig.slotName(),
                            connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }

        return Lsn.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void readTableStructure(
            ChangeEventSourceContext sourceContext,
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext,
            KingbaseOffsetContext offsetContext)
            throws SQLException, InterruptedException {
        Set<String> schemas =
                snapshotContext.capturedTables.stream()
                        .map(TableId::schema)
                        .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the
        // schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException(
                        "Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext, Table table)
            throws SQLException {
        return new SchemaChangeEvent(
                new HashMap<>(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    @Override
    protected void complete(SnapshotContext<KingbaseOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext, TableId tableId) {
        return snapshotter.buildSnapshotQuery(tableId, null);
    }

    protected void setSnapshotTransactionIsolationLevel() throws SQLException {
        LOGGER.info("Setting isolation level");
        String transactionStatement =
                snapshotter.snapshotTransactionIsolationLevelStatement(slotCreatedInfo);
        LOGGER.info("Opening transaction with statement {}", transactionStatement);
        jdbcConnection.executeWithoutCommitting(transactionStatement);
    }

    /** Mutable context which is populated in the course of snapshotting. */
    private static class PostgresSnapshotContext
            extends RelationalSnapshotContext<KingbaseOffsetContext> {

        public PostgresSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }

    /**
     * Generate truncate table message to push to the queue.
     *
     * @param snapshotContext RelationalSnapshotContext
     * @param receiver SnapshotReceiver
     * @throws InterruptedException Link break
     */
    private void pushTruncateMessageForTable(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext,
            EventDispatcher.SnapshotReceiver receiver,
            List<String> schemaList)
            throws InterruptedException {
        for (Iterator<TableId> iterator = snapshotContext.capturedTables.iterator();
                iterator.hasNext(); ) {
            final TableId tableId = iterator.next();
            boolean hasNext = iterator.hasNext();
            if (!schemaList.contains(tableId.schema())) {
                continue;
            }
            ChangeRecordEmitter truncateRecordEmitter =
                    getTruncateRecordEmitter(snapshotContext, tableId);
            dispatcher.dispatchSnapshotEvent(tableId, truncateRecordEmitter, receiver);
        }
    }

    private List<String> appointSchemas() throws SQLException {
        List<String> schemaList = new ArrayList<>();
        try (Connection connection = connectorConfig.getConnection(connectorConfig);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT pn.oid AS schema_oid, iss.catalog_name, iss.schema_owner, "
                                        + "iss.schema_name FROM information_schema.schemata iss "
                                        + "INNER JOIN pg_namespace pn ON pn.nspname = iss.schema_name "
                                        + "where iss.schema_name = 'public' or pn.oid > 16384;")) {
            while (rs.next()) {
                schemaList.add(rs.getString("schema_name"));
            }
        }
        return schemaList;
    }

    private BigInteger initCsvDirSize() {
        String csvPathSize = connectorConfig.getExportCsvPathSize();
        LOGGER.info("config: export.csv.path.size = {}", csvPathSize);
        if (isNumeric(csvPathSize)) {
            int size = Integer.parseInt(csvPathSize);
            BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
            return BigInteger.valueOf(size).multiply(unit).multiply(unit).multiply(unit);
        }
        return initSizeOfConfig(csvPathSize, csvDirSize);
    }

    private BigInteger initPagePartitionSize() {
        String exportFileSize = connectorConfig.getExportFileSize();
        LOGGER.info("config: export.file.size = {}", exportFileSize);
        if (isNumeric(exportFileSize)) {
            int size = Integer.parseInt(exportFileSize);
            BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
            return BigInteger.valueOf(size).multiply(unit).multiply(unit);
        }
        return initSizeOfConfig(exportFileSize, pageSize);
    }

    private BigInteger initSizeOfConfig(String sizeStr, BigInteger defaultValue) {
        String value = stringToInt(sizeStr);
        int len = sizeStr.length() - value.length();
        if (len > 1) {
            LOGGER.warn("config = {} invalid. Default value:{} byte", sizeStr, defaultValue);
            return defaultValue;
        }
        int size = Integer.parseInt(value);
        return initStoreSize(size, sizeStr, defaultValue);
    }

    private BigInteger initStoreSize(int size, String sizeStr, BigInteger defaultSize) {
        BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
        if (sizeStr.endsWith("K") || sizeStr.endsWith("k")) {
            return BigInteger.valueOf(size).multiply(unit);
        }
        if (sizeStr.endsWith("M") || sizeStr.endsWith("m")) {
            return BigInteger.valueOf(size).multiply(unit).multiply(unit);
        }
        if (sizeStr.endsWith("G") || sizeStr.endsWith("g")) {
            return BigInteger.valueOf(size).multiply(unit).multiply(unit).multiply(unit);
        }
        LOGGER.warn("config = {} invalid. Default value:{} byte", sizeStr, defaultSize);
        return defaultSize;
    }

    private boolean isNumeric(CharSequence cs) {
        int size = cs.length();
        if (size == 0) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private String stringToInt(CharSequence cs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cs.length(); i++) {
            if (Character.isDigit(cs.charAt(i))) {
                sb.append(cs.charAt(i));
            }
        }
        return sb.toString();
    }

    private ChangeRecordEmitter getTruncateRecordEmitter(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext, TableId tableId) {
        ReplicationMessage message =
                new ReplicationMessage() {
                    @Override
                    public Operation getOperation() {
                        return Operation.TRUNCATE;
                    }

                    @Override
                    public Instant getCommitTime() {
                        return clock.currentTime();
                    }

                    @Override
                    public OptionalLong getTransactionId() {
                        return OptionalLong.empty();
                    }

                    @Override
                    public String getTable() {
                        return null;
                    }

                    @Override
                    public List<Column> getOldTupleList() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<Column> getNewTupleList() {
                        return new ArrayList<>();
                    }

                    @Override
                    public boolean hasTypeMetadata() {
                        return false;
                    }

                    @Override
                    public boolean isLastEventForLsn() {
                        return false;
                    }
                };
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new TruncateRecordEmitter(
                snapshotContext.offset,
                getClock(),
                connectorConfig,
                schema,
                jdbcConnection,
                tableId,
                message);
    }

    private void wirteAndSendData(
            KingbaseDataEventsParam dataEventsParam,
            List<String> columnStringArr,
            int subscript,
            String columnString)
            throws IOException, InterruptedException {
        Table table = dataEventsParam.getTable();
        RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext =
                dataEventsParam.getSnapshotContext();
        EventDispatcher.SnapshotReceiver snapshotReceiver = dataEventsParam.getSnapshotReceiver();
        String path = generateFileName(table.id().schema(), table.id().table(), subscript);
        if (wirteCsv(columnStringArr, path)) {
            synchronized (messLock) {
                ChangeRecordEmitter changeRecordEmitter =
                        getFilePathRecordEmitter(
                                snapshotContext, table.id(), new String[] {path, columnString});
                dispatcher.dispatchSnapshotEvent(table.id(), changeRecordEmitter, snapshotReceiver);
            }
        }
    }

    private String generateFileName(String schema, String table, int subscript) {
        return new File(csvPath)
                + File.separator
                + String.format(Locale.ROOT, "%s_%s_%d.csv", schema, table, subscript);
    }

    private ChangeRecordEmitter getFilePathRecordEmitter(
            RelationalSnapshotContext<KingbaseOffsetContext> snapshotContext,
            TableId tableId,
            String[] row) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotChangeFilePathRecordEmitter(snapshotContext.offset, getClock(), row);
    }

    private String columnToString(ResultSet rs, ColumnUtils.ColumnArray columnArray, Table table)
            throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        TableSchema tableSchema;
        if (dispatcher.getSchema().schemaFor(table.id()) instanceof TableSchema) {
            tableSchema = (TableSchema) dispatcher.getSchema().schemaFor(table.id());
        } else {
            throw new DebeziumException("Kingbase2mysql full data schema error");
        }
        Struct newValue =
                tableSchema.valueFromColumnData(
                        jdbcConnection.rowToArray(table, schema(), rs, columnArray));
        int len = columnArray.getColumns().length;
        for (int i = 0; i < len; i++) {
            Object value = getValue(columnArray.getColumns()[i], newValue);
            if (value instanceof ByteBuffer) {
                ByteBuffer object = (ByteBuffer) value;
                value =
                        new String(
                                object.array(),
                                object.position(),
                                object.limit(),
                                Charset.defaultCharset());
            }
            if (value instanceof byte[]) {
                StringBuilder bytes = new StringBuilder();
                byte[] obj = (byte[]) value;
                for (byte b : obj) {
                    bytes.append(String.valueOf(b));
                }
                value = bytes.toString();
            }
            stringBuilder.append(value);
            if (i != len - 1) {
                stringBuilder.append(DELIMITER);
            }
        }
        return stringBuilder.toString();
    }

    private Object getValue(Column column, Struct newValue) {
        String columnName = column.name();
        int oid = column.jdbcType();
        Object value;
        switch (oid) {
            case Types.BLOB:
                byte[] bytes = newValue.getBytes(columnName);
                value = new String(bytes);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                value = newValue.getBytes(columnName);
                break;
            default:
                value = newValue.get(columnName);
        }
        return value;
    }

    private boolean wirteCsv(List<String> columnStringArr, String path) throws IOException {
        if (columnStringArr.isEmpty()) {
            return false;
        }
        blockWriteFile();
        File file = new File(path);
        try (FileOutputStream fileInputStream = new FileOutputStream(file); ) {
            PrintWriter printWriter = new PrintWriter(fileInputStream, true);
            String data =
                    String.join(System.lineSeparator(), columnStringArr) + System.lineSeparator();
            printWriter.write(data);
            printWriter.flush();
        } catch (IOException e) {
            throw new IOException(e);
        }
        return true;
    }

    private void blockWriteFile() {
        if (csvDirSize == null) {
            return;
        }
        LOGGER.warn(
                "csvDir capacity check. Write directly when conditions are met, Otherwise, "
                        + "wait to write when satisfied");
        for (; ; ) {
            long csvDir = getCsvDir();
            if (csvDir < csvDirSize.longValue()) {
                break;
            }
        }
    }

    private long getCsvDir() {
        synchronized (dirLock) {
            return FileUtils.sizeOfDirectory(new File(csvPath));
        }
    }

    private Statement readTableStatementKingbase(Connection connection) throws SQLException {
        return jdbcConnection.readTableStatementKingbase(connectorConfig, connection);
    }
}
