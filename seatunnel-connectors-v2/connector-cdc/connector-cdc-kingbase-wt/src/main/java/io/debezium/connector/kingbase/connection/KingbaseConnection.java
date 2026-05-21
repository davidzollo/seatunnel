/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbase.connection;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kingbase8.core.BaseConnection;
import com.kingbase8.jdbc.TimestampUtils;
import com.kingbase8.replication.LogSequenceNumber;
import com.kingbase8.util.KBmoney;
import com.kingbase8.util.KSQLState;
import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.KingbaseOid;
import io.debezium.connector.kingbase.KingbaseSchema;
import io.debezium.connector.kingbase.KingbaseType;
import io.debezium.connector.kingbase.KingbaseValueConverter;
import io.debezium.connector.kingbase.TypeRegistry;
import io.debezium.connector.kingbase.spi.SlotState;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Metronome;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link JdbcConnection} connection extension used for connecting to Postgres instances.
 *
 * @author Horia Chiorean
 */
public class KingbaseConnection extends JdbcConnection {

    /** Raw JDBC URL propagated from SeaTunnel so nested Debezium connections reuse it verbatim. */
    private static final Field URL = Field.create("url", "Raw JDBC url");

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private static Logger LOGGER = LoggerFactory.getLogger(KingbaseConnection.class);

    private static final String URL_PATTERN =
            "jdbc:kingbase8://${"
                    + JdbcConfiguration.HOSTNAME
                    + "}:${"
                    + JdbcConfiguration.PORT
                    + "}/${"
                    + JdbcConfiguration.DATABASE
                    + "}";
    protected static final ConnectionFactory FACTORY =
            JdbcConnection.patternBasedFactory(
                    URL_PATTERN,
                    com.kingbase8.Driver.class.getName(),
                    KingbaseConnection.class.getClassLoader(),
                    JdbcConfiguration.PORT.withDefault(
                            KingbaseConnectorConfig.PORT.defaultValueAsString()));

    /**
     * Obtaining a replication slot may fail if there's a pending transaction. We're retrying to get
     * a slot for 30 min.
     */
    private static final int MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT = 900;

    private static final Duration PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS =
            Duration.ofSeconds(2);

    /** Probes the PostgreSQL-compatible catalog objects used by the CDC relation metadata path. */
    private static final String PG_CATALOG_PROBE_SQL = "SELECT 1 FROM pg_constraint LIMIT 1";

    /** Reads primary key columns from PostgreSQL-compatible catalogs when they are available. */
    private static final String SELECT_PRIMARY_KEYS_SQL =
            "SELECT con.conname AS pk_name, a.attname AS column_name, ord.key_seq AS key_seq "
                    + "FROM pg_constraint con "
                    + "JOIN pg_class cls ON cls.oid = con.conrelid "
                    + "JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN unnest(con.conkey::int[]) WITH ORDINALITY AS ord(attnum, key_seq) ON TRUE "
                    + "JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE con.contype = 'p' AND nsp.nspname = ? AND cls.relname = ? "
                    + "ORDER BY ord.key_seq";

    /**
     * Reads primary key columns from the legacy sys_* catalogs on Kingbase builds without the
     * PostgreSQL-compatible catalog view.
     */
    private static final String SELECT_PRIMARY_KEYS_SYS_SQL =
            "SELECT con.conname AS pk_name, a.attname AS column_name, ord.key_seq AS key_seq "
                    + "FROM sys_constraint con "
                    + "JOIN sys_class cls ON cls.oid = con.conrelid "
                    + "JOIN sys_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN unnest(con.conkey::int[]) WITH ORDINALITY AS ord(attnum, key_seq) ON TRUE "
                    + "JOIN sys_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE con.contype = 'p' AND nsp.nspname = ? AND cls.relname = ? "
                    + "ORDER BY ord.key_seq";

    /**
     * Keeps the same first-valid-unique-index semantics as JdbcConnection while using
     * PostgreSQL-compatible catalogs.
     */
    private static final String SELECT_UNIQUE_INDEXES_SQL =
            "SELECT idx.relname AS index_name, a.attname AS column_name, ord.column_index AS column_index "
                    + "FROM pg_index i "
                    + "JOIN pg_class cls ON cls.oid = i.indrelid "
                    + "JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN pg_class idx ON idx.oid = i.indexrelid "
                    + "JOIN unnest(i.indkey::int[]) WITH ORDINALITY AS ord(attnum, column_index) ON TRUE "
                    + "JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE i.indisunique = TRUE AND i.indisprimary = FALSE "
                    + "AND ord.attnum > 0 AND nsp.nspname = ? AND cls.relname = ? "
                    + "ORDER BY idx.relname, ord.column_index";

    /**
     * Keeps the same first-valid-unique-index semantics as JdbcConnection while using the legacy
     * sys_* catalogs.
     */
    private static final String SELECT_UNIQUE_INDEXES_SYS_SQL =
            "SELECT idx.relname AS index_name, a.attname AS column_name, ord.column_index AS column_index "
                    + "FROM sys_index i "
                    + "JOIN sys_class cls ON cls.oid = i.indrelid "
                    + "JOIN sys_namespace nsp ON nsp.oid = cls.relnamespace "
                    + "JOIN sys_class idx ON idx.oid = i.indexrelid "
                    + "JOIN unnest(i.indkey::int[]) WITH ORDINALITY AS ord(attnum, column_index) ON TRUE "
                    + "JOIN sys_attribute a ON a.attrelid = cls.oid AND a.attnum = ord.attnum "
                    + "WHERE i.indisunique = TRUE AND i.indisprimary = FALSE "
                    + "AND ord.attnum > 0 AND nsp.nspname = ? AND cls.relname = ? "
                    + "ORDER BY idx.relname, ord.column_index";

    /** Cached decision for whether the current server exposes PostgreSQL-compatible catalogs. */
    private volatile Boolean usePostgresCatalog;

    private final TypeRegistry typeRegistry;
    private final KingbaseDefaultValueConverter defaultValueConverter;

    /**
     * Creates a Postgres connection using the supplied configuration. If necessary this connection
     * is able to resolve data type mappings. Such a connection requires a {@link
     * KingbaseValueConverter}, and will provide its own {@link TypeRegistry}. Usually only one such
     * connection per connector is needed.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param valueConverterBuilder supplies a configured {@link KingbaseValueConverter} for a given
     *     {@link TypeRegistry}
     */
    public KingbaseConnection(
            Configuration config, KingbaseValueConverterBuilder valueConverterBuilder) {
        super(config, resolveConnectionFactory(config), null, KingbaseConnection::defaultSettings);
        if (Objects.isNull(valueConverterBuilder)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        } else {
            this.typeRegistry = new TypeRegistry(this);

            final KingbaseValueConverter valueConverter =
                    valueConverterBuilder.build(this.typeRegistry);
            this.defaultValueConverter =
                    new KingbaseDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    /**
     * Create a Postgres connection using the supplied configuration and {@link TypeRegistry}
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param typeRegistry an existing/already-primed {@link TypeRegistry} instance
     */
    public KingbaseConnection(KingbaseConnectorConfig config, TypeRegistry typeRegistry) {
        super(
                config.getJdbcConfig(),
                resolveConnectionFactory(config.getJdbcConfig()),
                null,
                KingbaseConnection::defaultSettings);
        if (Objects.isNull(typeRegistry)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        } else {
            this.typeRegistry = typeRegistry;
            final KingbaseValueConverter valueConverter =
                    KingbaseValueConverter.of(config, this.getDatabaseCharset(), typeRegistry);
            this.defaultValueConverter =
                    new KingbaseDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    /**
     * Creates a Postgres connection using the supplied configuration. The connector is the regular
     * one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public KingbaseConnection(Configuration config) {
        this(config, null);
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values
     *     from the configuration
     */
    public String connectionString() {
        String rawJdbcUrl = config().getString(URL);
        if (rawJdbcUrl != null && !rawJdbcUrl.isEmpty()) {
            return rawJdbcUrl;
        }
        return connectionString(URL_PATTERN);
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table. This in turn determines
     * how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the
     *     given table
     */
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement =
                "SELECT relreplident FROM pg_catalog.pg_class c "
                        + "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid "
                        + "WHERE n.nspname=? and c.relname=?";
        String schema =
                tableId.schema() != null && tableId.schema().length() > 0
                        ? tableId.schema()
                        : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(
                statement,
                stmt -> {
                    stmt.setString(1, schema);
                    stmt.setString(2, tableId.table());
                },
                rs -> {
                    if (rs.next()) {
                        replIdentity.append(rs.getString(1));
                    } else {
                        LOGGER.warn(
                                "Cannot determine REPLICA IDENTITY information for table '{}'",
                                tableId);
                    }
                });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }

    /**
     * Returns the current state of the replication slot
     *
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link SlotState} or null, if no slot state is found
     * @throws SQLException
     */
    public SlotState getReplicationSlotState(String slotName, String pluginName)
            throws SQLException {
        ServerInfo.ReplicationSlot slot;
        try {
            slot = readReplicationSlotInfo(slotName, pluginName);
            if (slot.equals(ServerInfo.ReplicationSlot.INVALID)) {
                return null;
            } else {
                return slot.asSlotState();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException(
                    "Interrupted while waiting for valid replication slot info", e);
        }
    }

    /**
     * Fetches the state of a replication stage given a slot name and plugin name
     *
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link
     *     ServerInfo.ReplicationSlot#INVALID} if the slot is not valid
     * @throws SQLException is thrown by the underlying JDBC
     */
    private ServerInfo.ReplicationSlot fetchReplicationSlotInfo(String slotName, String pluginName)
            throws SQLException {
        final String database = database();
        final ServerInfo.ReplicationSlot slot =
                queryForSlot(
                        slotName,
                        database,
                        pluginName,
                        rs -> {
                            if (rs.next()) {
                                boolean active = rs.getBoolean("active");
                                final Lsn confirmedFlushedLsn =
                                        parseConfirmedFlushLsn(slotName, pluginName, database, rs);
                                if (confirmedFlushedLsn == null) {
                                    return null;
                                }
                                Lsn restartLsn =
                                        parseRestartLsn(slotName, pluginName, database, rs);
                                if (restartLsn == null) {
                                    return null;
                                }
                                final Long xmin = rs.getLong("catalog_xmin");
                                return new ServerInfo.ReplicationSlot(
                                        active, confirmedFlushedLsn, restartLsn, xmin);
                            } else {
                                LOGGER.debug(
                                        "No replication slot '{}' is present for plugin '{}' and database '{}'",
                                        slotName,
                                        pluginName,
                                        database);
                                return ServerInfo.ReplicationSlot.INVALID;
                            }
                        });
        return slot;
    }

    /**
     * Fetches a replication slot, repeating the query until either the slot is created or until the
     * max number of attempts has been reached
     *
     * <p>To fetch the slot without the retries, use the {@link
     * KingbaseConnection#fetchReplicationSlotInfo} call
     *
     * @param slotName the slot name
     * @param pluginName the name of the plugin
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link
     *     ServerInfo.ReplicationSlot#INVALID} if the slot is not valid
     * @throws SQLException is thrown by the underyling jdbc driver
     * @throws InterruptedException is thrown if we don't return an answer within the set number of
     *     retries
     */
    @VisibleForTesting
    ServerInfo.ReplicationSlot readReplicationSlotInfo(String slotName, String pluginName)
            throws SQLException, InterruptedException {
        final String database = database();
        final Metronome metronome =
                Metronome.parker(PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS, Clock.SYSTEM);

        for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT; attempt++) {
            final ServerInfo.ReplicationSlot slot = fetchReplicationSlotInfo(slotName, pluginName);
            if (slot != null) {
                LOGGER.info("Obtained valid replication slot {}", slot);
                return slot;
            }
            LOGGER.warn(
                    "Cannot obtain valid replication slot '{}' for plugin '{}' and database '{}' [during attempt {} out of {}, concurrent tx probably blocks taking snapshot.",
                    slotName,
                    pluginName,
                    database,
                    attempt,
                    MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT);
            metronome.pause();
        }

        throw new ConnectException(
                "Unable to obtain valid replication slot. "
                        + "Make sure there are no long-running transactions running in parallel as they may hinder the allocation of the replication slot when starting this connector");
    }

    protected ServerInfo.ReplicationSlot queryForSlot(
            String slotName,
            String database,
            String pluginName,
            ResultSetMapper<ServerInfo.ReplicationSlot> map)
            throws SQLException {
        return prepareQueryAndMap(
                "select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?",
                statement -> {
                    statement.setString(1, slotName);
                    statement.setString(2, database);
                    statement.setString(3, pluginName);
                },
                map);
    }

    /**
     * Obtains the LSN to resume streaming from. On PG 9.5 there is no confirmed_flushed_lsn yet, so
     * restart_lsn will be read instead. This may result in more records to be re-read after a
     * restart.
     */
    private Lsn parseConfirmedFlushLsn(
            String slotName, String pluginName, String database, ResultSet rs) {
        Lsn confirmedFlushedLsn = null;

        try {
            confirmedFlushedLsn =
                    tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush_lsn");
        } catch (SQLException e) {
            LOGGER.info("unable to find confirmed_flushed_lsn, falling back to restart_lsn");
            try {
                confirmedFlushedLsn =
                        tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush");
            } catch (SQLException e2) {
                try {
                    confirmedFlushedLsn =
                            tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
                } catch (SQLException e3) {
                    throw new ConnectException(
                            "Neither confirmed_flush_lsn nor restart_lsn could be found", e3);
                }
            }
        }

        return confirmedFlushedLsn;
    }

    private Lsn parseRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn restartLsn = null;
        try {
            restartLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        } catch (SQLException e) {
            throw new ConnectException("restart_lsn could be found");
        }

        return restartLsn;
    }

    private Lsn tryParseLsn(
            String slotName, String pluginName, String database, ResultSet rs, String column)
            throws ConnectException, SQLException {
        Lsn lsn = null;

        String lsnStr = rs.getString(column);
        if (lsnStr == null) {
            return null;
        }
        try {
            lsn = Lsn.valueOf(lsnStr);
        } catch (Exception e) {
            throw new ConnectException(
                    "Value "
                            + column
                            + " in the pg_replication_slots table for slot = '"
                            + slotName
                            + "', plugin = '"
                            + pluginName
                            + "', database = '"
                            + database
                            + "' is not valid. This is an abnormal situation and the database status should be checked.");
        }
        if (!lsn.isValid()) {
            throw new ConnectException("Invalid LSN returned from database");
        }
        return lsn;
    }

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    public boolean dropReplicationSlot(String slotName) {
        final int ATTEMPTS = 3;
        for (int i = 0; i < ATTEMPTS; i++) {
            try {
                execute("select pg_drop_replication_slot('" + slotName + "')");
                return true;
            } catch (SQLException e) {
                // slot is active
                if (KSQLState.OBJECT_IN_USE.getState().equals(e.getSQLState())) {
                    if (i < ATTEMPTS - 1) {
                        LOGGER.debug(
                                "Cannot drop replication slot '{}' because it's still in use",
                                slotName);
                    } else {
                        LOGGER.warn(
                                "Cannot drop replication slot '{}' because it's still in use",
                                slotName);
                        return false;
                    }
                } else if (KSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                    LOGGER.debug("Replication slot {} has already been dropped", slotName);
                    return false;
                } else {
                    LOGGER.error("Unexpected error while attempting to drop replication slot", e);
                    return false;
                }
            }
            try {
                Metronome.parker(Duration.ofSeconds(1), Clock.system()).pause();
            } catch (InterruptedException e) {
            }
        }
        return false;
    }

    /**
     * Drops the debezium publication that was created.
     *
     * @param publicationName the publication name, may not be null
     * @return {@code true} if the publication was dropped, {@code false} otherwise
     */
    public boolean dropPublication(String publicationName) {
        try {
            LOGGER.debug("Dropping publication '{}'", publicationName);
            execute("DROP PUBLICATION " + publicationName);
            return true;
        } catch (SQLException e) {
            if (KSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Publication {} has already been dropped", publicationName);
            } else {
                LOGGER.error("Unexpected error while attempting to drop publication", e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        } catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
    }

    /**
     * Returns the PG id of the current active transaction
     *
     * @return a PG transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        query(
                "select * from txid_current()",
                rs -> {
                    if (rs.next()) {
                        txId.compareAndSet(0, rs.getLong(1));
                    }
                });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(
                majorVersion >= 10
                        ? "select * from pg_current_wal_lsn()"
                        : "select * from pg_current_xlog_location()",
                rs -> {
                    if (!rs.next()) {
                        throw new IllegalStateException(
                                "there should always be a valid xlog position");
                    }
                    result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
                });
        return result.get();
    }

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query(
                "SELECT version(), current_user, current_database()",
                rs -> {
                    if (rs.next()) {
                        serverInfo
                                .withServer(rs.getString(1))
                                .withUsername(rs.getString(2))
                                .withDatabase(rs.getString(3));
                    }
                });
        return serverInfo;
    }

    public Charset getDatabaseCharset() {
        try {
            return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        } catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain encoding for database " + database(), e);
        }
    }

    public TimestampUtils getTimestampUtils() {
        try {
            return ((BaseConnection) this.connection()).getTimestampUtils();
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Couldn't get timestamp utils from underlying connection", e);
        }
    }

    protected static void defaultSettings(Configuration.Builder builder) {
        // we require Postgres 9.4 as the minimum server version since that's where logical
        // replication was first introduced
        builder.with("assumeMinServerVersion", "9.4");
    }

    /**
     * Prefer the raw JDBC URL from the outer SeaTunnel datasource so Debezium's secondary metadata
     * connections reuse the same host bridge and transport flags.
     */
    private static ConnectionFactory resolveConnectionFactory(Configuration config) {
        String rawJdbcUrl = config.getString(URL);
        if (rawJdbcUrl == null || rawJdbcUrl.isEmpty()) {
            return FACTORY;
        }
        return JdbcConnection.patternBasedFactory(
                rawJdbcUrl,
                com.kingbase8.Driver.class.getName(),
                KingbaseConnection.class.getClassLoader(),
                JdbcConfiguration.PORT.withDefault(
                        KingbaseConnectorConfig.PORT.defaultValueAsString()));
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    @Override
    public List<String> readPrimaryKeyNames(DatabaseMetaData metadata, TableId id)
            throws SQLException {
        if (shouldUsePostgresCatalog()) {
            try {
                return readPrimaryKeyNames(id, SELECT_PRIMARY_KEYS_SQL);
            } catch (SQLException e) {
                LOGGER.info(
                        "Falling back to sys_* primary key catalogs for table {} because pg_* lookup failed: {}",
                        id,
                        e.getMessage());
                usePostgresCatalog = false;
            }
        }
        return readPrimaryKeyNames(id, SELECT_PRIMARY_KEYS_SYS_SQL);
    }

    /**
     * Reads primary key column names from the selected catalog family while preserving ordinal
     * order for composite keys.
     */
    private List<String> readPrimaryKeyNames(TableId id, String sql) throws SQLException {
        final List<String> pkColumnNames = new ArrayList<>();
        try (PreparedStatement statement = connection().prepareStatement(sql)) {
            statement.setString(1, normalizeSchema(id));
            statement.setString(2, id.table());
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    int columnIndex = rs.getInt("key_seq");
                    Collect.set(pkColumnNames, columnIndex - 1, columnName, null);
                }
            }
        }
        return pkColumnNames;
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id)
            throws SQLException {
        if (shouldUsePostgresCatalog()) {
            try {
                return readTableUniqueIndices(id, SELECT_UNIQUE_INDEXES_SQL);
            } catch (SQLException e) {
                LOGGER.info(
                        "Falling back to sys_* unique index catalogs for table {} because pg_* lookup failed: {}",
                        id,
                        e.getMessage());
                usePostgresCatalog = false;
            }
        }
        return readTableUniqueIndices(id, SELECT_UNIQUE_INDEXES_SYS_SQL);
    }

    /**
     * Reads the first valid unique index column list from the selected catalog family, matching
     * JdbcConnection's uniqueness semantics.
     */
    private List<String> readTableUniqueIndices(TableId id, String sql) throws SQLException {
        final List<String> uniqueIndexColumnNames = new ArrayList<>();
        final Set<String> excludedIndexNames = new HashSet<>();
        try (PreparedStatement statement = connection().prepareStatement(sql)) {
            statement.setString(1, normalizeSchema(id));
            statement.setString(2, id.table());
            try (ResultSet rs = statement.executeQuery()) {
                String firstIndexName = null;
                while (rs.next()) {
                    final String indexName = rs.getString("index_name");
                    final String columnName = rs.getString("column_name");
                    final int columnIndex = rs.getInt("column_index");

                    if (indexName == null || excludedIndexNames.contains(indexName)) {
                        continue;
                    }

                    boolean indexIncluded = isTableUniqueIndexIncluded(indexName, columnName);
                    if (!indexIncluded) {
                        excludedIndexNames.add(indexName);
                        if (firstIndexName == null || indexName.equals(firstIndexName)) {
                            firstIndexName = null;
                            uniqueIndexColumnNames.clear();
                            continue;
                        }
                    }

                    if (firstIndexName == null) {
                        firstIndexName = indexName;
                    }

                    if (!indexName.equals(firstIndexName)) {
                        return uniqueIndexColumnNames;
                    }

                    if (columnName != null) {
                        Collect.set(uniqueIndexColumnNames, columnIndex - 1, columnName, null);
                    }
                }
            }
        }
        return uniqueIndexColumnNames;
    }

    /**
     * Determines once per connection whether the server exposes the PostgreSQL-compatible catalogs
     * required by the pgoutput relation metadata path.
     */
    @VisibleForTesting
    protected boolean shouldUsePostgresCatalog() throws SQLException {
        if (usePostgresCatalog == null) {
            usePostgresCatalog = probePostgresCatalog();
        }
        return usePostgresCatalog;
    }

    /** Probes the minimal pg_* catalog surface that relation metadata lookups depend on. */
    private boolean probePostgresCatalog() {
        try (Statement statement = connection().createStatement();
                ResultSet ignored = statement.executeQuery(PG_CATALOG_PROBE_SQL)) {
            return true;
        } catch (SQLException e) {
            LOGGER.info(
                    "KingbaseConnection falling back to legacy sys_* catalogs because pg_* compatibility probe failed: {}",
                    e.getMessage());
            return false;
        }
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(
            ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnFilter);
    }

    public Optional<Column> readColumnForDecoder(
            ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnNameFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnNameFilter)
                .map(ColumnEditor::create);
    }

    private Optional<ColumnEditor> doReadTableColumn(
            ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null
                || columnFilter.matches(
                        tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));

            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            } catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final KingbaseType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (TypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }

            final String defaultValueExpression = columnMetadata.getString(13);
            return Optional.of(column);
        }

        return Optional.empty();
    }

    public KingbaseDefaultValueConverter getDefaultValueConverter() {
        Objects.requireNonNull(
                defaultValueConverter, "Connection does not provide default value converter");
        return defaultValueConverter;
    }

    public TypeRegistry getTypeRegistry() {
        Objects.requireNonNull(typeRegistry, "Connection does not provide type registry");
        return typeRegistry;
    }

    @Override
    public <T extends DatabaseSchema<TableId>> Object getColumnValue(
            ResultSet rs, int columnIndex, Column column, Table table, T schema)
            throws SQLException {
        try {
            if (column.nativeType() == KingbaseOid.BIT
                    || column.nativeType() == KingbaseOid.VARBIT) {
                // Kingbase JDBC can route BIT values through boolean accessors during snapshots.
                // Reading the raw bit string keeps BIT(n) values such as B'1010' lossless.
                return rs.getString(columnIndex);
            }

            final ResultSetMetaData metaData = rs.getMetaData();
            final String columnTypeName = metaData.getColumnTypeName(columnIndex);
            final KingbaseType type =
                    ((KingbaseSchema) schema).getTypeRegistry().get(columnTypeName);

            LOGGER.trace("Type of incoming data is: {}", type.getOid());
            LOGGER.trace("ColumnTypeName is: {}", columnTypeName);
            LOGGER.trace("Type is: {}", type);

            if (type.isArrayType()) {
                return rs.getArray(columnIndex);
            }

            switch (type.getOid()) {
                case KingbaseOid.MONEY:
                    // TODO author=Horia Chiorean date=14/11/2016 description=workaround for
                    // https://github.com/pgjdbc/pgjdbc/issues/100
                    final String sMoney = rs.getString(columnIndex);
                    if (sMoney == null) {
                        return sMoney;
                    }
                    if (sMoney.startsWith("-")) {
                        // KBmoney expects negative values to be provided in the format of
                        // "($XXXXX.YY)"
                        final String negativeMoney = "(" + sMoney.substring(1) + ")";
                        return KingbaseMoneyUtil.numericValue(new KBmoney(negativeMoney));
                    }
                    return KingbaseMoneyUtil.numericValue(new KBmoney(sMoney));
                case KingbaseOid.BIT:
                    return rs.getString(columnIndex);
                case KingbaseOid.NUMERIC:
                    final String s = rs.getString(columnIndex);
                    if (s == null) {
                        return s;
                    }

                    Optional<SpecialValueDecimal> value = KingbaseValueConverter.toSpecialValue(s);
                    return value.isPresent()
                            ? value.get()
                            : new SpecialValueDecimal(rs.getBigDecimal(columnIndex));
                case KingbaseOid.TIME:
                    // To handle time 24:00:00 supported by TIME columns, read the column as a
                    // string.
                case KingbaseOid.TIMETZ:
                    // In order to guarantee that we resolve TIMETZ columns with proper microsecond
                    // precision,
                    // read the column as a string instead and then re-parse inside the converter.
                    return rs.getString(columnIndex);
                default:
                    Object x = rs.getObject(columnIndex);
                    if (x != null) {
                        LOGGER.trace(
                                "rs getobject returns class: {}; rs getObject value is: {}",
                                x.getClass(),
                                x);
                    }
                    return x;
            }
        } catch (SQLException e) {
            // not a known type
            return super.getColumnValue(rs, columnIndex, column, table, schema);
        }
    }

    /**
     * Create a statement for the database session
     *
     * @param connectorConfig Kingbase source config
     * @param connection Kingbase Connection
     * @return Statement
     * @throws SQLException createStatement exception
     */
    public Statement readTableStatementKingbase(
            KingbaseConnectorConfig connectorConfig, Connection connection) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        final Statement statement =
                connection.createStatement(); // the default cursor is FORWARD_ONLY
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private String normalizeSchema(TableId id) {
        return id.schema() == null || id.schema().isEmpty() ? "public" : id.schema();
    }

    @FunctionalInterface
    public interface KingbaseValueConverterBuilder {
        KingbaseValueConverter build(TypeRegistry registry);
    }
}
