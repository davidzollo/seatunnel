/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.kingbase.connection.Lsn;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@NotThreadSafe
public final class SourceInfo extends BaseSourceInfo {

    public static final String TIMESTAMP_USEC_KEY = "ts_usec";
    public static final String TXID_KEY = "txId";
    public static final String XMIN_KEY = "xmin";
    public static final String LSN_KEY = "lsn";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String dbName;

    private Lsn lsn;
    private Lsn lastCommitLsn;
    private Long txId;
    private Long xmin;
    private Instant timestamp;
    private String schemaName;
    private String tableName;

    protected SourceInfo(KingbaseConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.dbName = connectorConfig.databaseName();
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param lsn the position in the server WAL for a particular event; may be null indicating that
     *     this information is not available
     * @param commitTime the commit time of the transaction that generated the event; may be null
     *     indicating that this information is not available
     * @param txId the ID of the transaction that generated the transaction; may be null if this
     *     information is not available
     * @param xmin the xmin of the slot, may be null
     * @param tableId the table that should be included in the source info; may be null
     * @return this instance
     */
    protected SourceInfo update(
            Lsn lsn, Instant commitTime, Long txId, Long xmin, TableId tableId) {
        this.lsn = lsn;
        if (commitTime != null) {
            this.timestamp = commitTime;
        }
        this.txId = txId;
        this.xmin = xmin;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        } else {
            this.schemaName = "";
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        } else {
            this.tableName = "";
        }
        return this;
    }

    // TODO https://issues.redhat.com/browse/DBZ-4329, make this call the method above, so to reset
    // the attributes not provided here
    protected SourceInfo update(Instant timestamp, TableId tableId) {
        this.timestamp = timestamp;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    /** Updates the source with the LSN of the last committed transaction. */
    protected SourceInfo updateLastCommit(Lsn lsn) {
        this.lastCommitLsn = lsn;
        this.lsn = lsn;
        return this;
    }

    public Lsn lsn() {
        return this.lsn;
    }

    public Long xmin() {
        return this.xmin;
    }

    @Override
    public String sequence() {
        List<String> sequence = new ArrayList<String>(2);
        String lastCommitLsn =
                (this.lastCommitLsn != null) ? Long.toString(this.lastCommitLsn.asLong()) : null;
        String lsn = (this.lsn != null) ? Long.toString(this.lsn.asLong()) : null;
        sequence.add(lastCommitLsn);
        sequence.add(lsn);
        try {
            return MAPPER.writeValueAsString(sequence);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected String database() {
        return dbName;
    }

    String schemaName() {
        return schemaName;
    }

    String tableName() {
        return tableName;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    protected Long txId() {
        return txId;
    }

    @Override
    public SnapshotRecord snapshot() {
        return super.snapshot();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("source_info[");
        sb.append("server='").append(serverName()).append('\'');
        sb.append("db='").append(dbName).append('\'');
        if (lsn != null) {
            sb.append(", lsn=").append(lsn);
        }
        if (txId != null) {
            sb.append(", txId=").append(txId);
        }
        if (xmin != null) {
            sb.append(", xmin=").append(xmin);
        }
        if (lastCommitLsn != null) {
            sb.append(", lastCommitLsn=").append(lastCommitLsn);
        }
        if (timestamp != null) {
            sb.append(", timestamp=").append(timestamp);
        }
        sb.append(", snapshot=").append(snapshot());
        if (schemaName != null) {
            sb.append(", schema=").append(schemaName);
        }
        if (tableName != null) {
            sb.append(", table=").append(tableName);
        }
        sb.append(']');
        return sb.toString();
    }
}
