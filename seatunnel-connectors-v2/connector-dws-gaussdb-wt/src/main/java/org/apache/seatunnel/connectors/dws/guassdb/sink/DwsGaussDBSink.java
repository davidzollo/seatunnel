/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.dws.guassdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption;
import org.apache.seatunnel.connectors.dws.guassdb.config.DwsGaussDBConfig;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkAggregatedCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.savemode.DwsGaussDBSaveModeHandler;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;
import org.apache.seatunnel.connectors.dws.guassdb.sink.writer.DwsGaussDBSinkWriter;
import org.apache.seatunnel.connectors.dws.guassdb.sink.writer.DwsGaussDBSinkWriterFactory;

import org.apache.commons.collections4.CollectionUtils;

import lombok.Getter;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.COPY_FILE_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.FIELD_IDE;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.PRIMARY_KEY;

public class DwsGaussDBSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        DwsGaussDBSinkState,
                        DwsGaussDBSinkCommitInfo,
                        DwsGaussDBSinkAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode {

    @Getter private final String pluginName = DwsGaussDBConfig.CONNECTOR_NAME;

    private SeaTunnelRowType seaTunnelRowType;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    private final DwsGaussSqlGenerator sqlGenerator;

    public DwsGaussDBSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        try {
            Class.forName(readonlyConfig.get(BaseDwsGaussDBOption.DRIVER));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.sqlGenerator =
                new DwsGaussSqlGenerator(
                        readonlyConfig.get(PRIMARY_KEY),
                        readonlyConfig.get(FIELD_IDE),
                        catalogTable,
                        readonlyConfig.get(COPY_FILE_FIELD_DELIMITER));
    }

    @Override
    public DwsGaussDBSinkWriter createWriter(SinkWriter.Context context) {
        try {
            try {
                Class.forName(readonlyConfig.get(BaseDwsGaussDBOption.DRIVER));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return DwsGaussDBSinkWriterFactory.createDwsGaussDBSinkWriter(
                    sqlGenerator, catalogTable, readonlyConfig);
        } catch (Exception ex) {
            throw new RuntimeException("Create SinkWriter failed", ex);
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState> restoreWriter(
            SinkWriter.Context context, List<DwsGaussDBSinkState> states) {
        try {
            try {
                Class.forName(readonlyConfig.get(BaseDwsGaussDBOption.DRIVER));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (CollectionUtils.isNotEmpty(states)) {
                try (DwsGaussDBCatalog dwsGaussDBCatalog =
                        new DwsGaussDBCatalogFactory()
                                .createCatalog(catalogTable.getCatalogName(), readonlyConfig)) {

                    List<Long> snapshotIds =
                            states.stream()
                                    .flatMap(state -> state.getSnapshotId().stream())
                                    .collect(Collectors.toList());

                    if (CollectionUtils.isNotEmpty(snapshotIds)) {
                        String deleteTemporarySnapshotSql =
                                sqlGenerator.getDeleteTemporarySnapshotSql(snapshotIds);
                        dwsGaussDBCatalog.executeUpdateSql(deleteTemporarySnapshotSql);
                    }
                }
            }
            return DwsGaussDBSinkWriterFactory.createDwsGaussDBRestoreWriter(
                    sqlGenerator, catalogTable, readonlyConfig, context, states);
        } catch (SQLException e) {
            throw new RuntimeException("Create SinkWriter failed", e);
        }
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<
                    SinkAggregatedCommitter<
                            DwsGaussDBSinkCommitInfo, DwsGaussDBSinkAggregatedCommitInfo>>
            createAggregatedCommitter() {
        try {
            Class.forName(readonlyConfig.get(BaseDwsGaussDBOption.DRIVER));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return Optional.of(
                new DwsGaussDBSinkAggregatedCommitter(sqlGenerator, catalogTable, readonlyConfig));
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkAggregatedCommitInfo>>
            getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        try {
            Class.forName(readonlyConfig.get(BaseDwsGaussDBOption.DRIVER));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        DwsGaussDBCatalog dwsGaussDBCatalog =
                new DwsGaussDBCatalogFactory()
                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig);
        return Optional.of(
                new DwsGaussDBSaveModeHandler(
                        readonlyConfig, catalogTable, dwsGaussDBCatalog, sqlGenerator));
    }
}
