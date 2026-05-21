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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.config.KingbaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.config.KingbaseSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.enumerator.KingbaseChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.KingbaseSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.snapshot.KingbaseSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.wal.KingbaseWalFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.KingbaseSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.connection.KingbaseConnection;
import io.debezium.connector.kingbase.connection.ServerInfo;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils.KingbaseConnectionUtils.newKingbaseValueConverterBuilder;

public class KingbaseDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final KingbaseSourceConfig sourceConfig;

    private transient KingbaseSchema kingbaseSchema;
    private final Map<TableId, CatalogTable> tableMap;
    private KingbaseWalFetchTask kingbaseWalFetchTask;

    public KingbaseDialect(
            KingbaseSourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {

        KingbaseConnectorConfig kingbaseConnectorConfig =
                (KingbaseConnectorConfig) sourceConfig.getDbzConnectorConfig();

        return new KingbaseConnection(
                kingbaseConnectorConfig.getJdbcConfig(),
                newKingbaseValueConverterBuilder(kingbaseConnectorConfig));
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new KingbaseChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        KingbaseSourceConfig kingbaseSourceConfig = (KingbaseSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            jdbcConnection, kingbaseSourceConfig.getTableFilters());
            this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            return tables;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public void checkAllTablesEnabledCapture(JdbcConnection jdbcConnection, List<TableId> tableIds)
            throws SQLException {
        KingbaseConnection kingbaseConnection = (KingbaseConnection) jdbcConnection;
        for (TableId tableId : tableIds) {
            ServerInfo.ReplicaIdentity replicaIdentity =
                    kingbaseConnection.readReplicaIdentityInfo(tableId);
            if (!ServerInfo.ReplicaIdentity.FULL.equals(replicaIdentity)) {
                throw new SeaTunnelException(
                        String.format(
                                "Table %s does not have a full replica identity, please execute: ALTER TABLE %s REPLICA IDENTITY FULL;",
                                tableId, tableId));
            }
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (kingbaseSchema == null) {
            kingbaseSchema = new KingbaseSchema(sourceConfig.getDbzConnectorConfig(), tableMap);
        }
        return kingbaseSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public KingbaseSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        KingbaseConnectorConfig kingbaseConnectorConfig =
                (KingbaseConnectorConfig) taskSourceConfig.getDbzConnectorConfig();

        final KingbaseConnection jdbcConnection =
                new KingbaseConnection(
                        kingbaseConnectorConfig.getJdbcConfig(),
                        newKingbaseValueConverterBuilder(kingbaseConnectorConfig));

        List<TableChanges.TableChange> tableChangeList = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            tableChangeList.add(queryTableSchema(jdbcConnection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            for (TableId tableId : incrementalSplit.getTableIds()) {
                tableChangeList.add(queryTableSchema(jdbcConnection, tableId));
            }
        }

        return new KingbaseSourceFetchTaskContext(
                taskSourceConfig, this, jdbcConnection, tableChangeList);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new KingbaseSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
                List<TableId> tables = sourceSplitBase.asIncrementalSplit().getTableIds();
                this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            } catch (SQLException e) {
                throw new SeaTunnelException("Error to check tables: " + e.getMessage(), e);
            }
            kingbaseWalFetchTask = new KingbaseWalFetchTask(sourceSplitBase.asIncrementalSplit());
            return kingbaseWalFetchTask;
        }
    }

    @Override
    public void commitChangeLogOffset(Offset offset) throws Exception {
        if (kingbaseWalFetchTask != null) {
            kingbaseWalFetchTask.commitCurrentOffset((LsnOffset) offset);
        }
    }

    @Override
    public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId) {
        return Optional.ofNullable(tableMap.get(tableId).getTableSchema().getPrimaryKey());
    }

    @Override
    public List<ConstraintKey> getConstraintKeys(JdbcConnection jdbcConnection, TableId tableId) {
        return tableMap.get(tableId).getTableSchema().getConstraintKeys();
    }
}
