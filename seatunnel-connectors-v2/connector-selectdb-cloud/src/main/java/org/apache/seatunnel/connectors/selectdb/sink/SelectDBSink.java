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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfoSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitter;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkState;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkStateSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkWriter;

import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

@NoArgsConstructor
public class SelectDBSink
        implements SeaTunnelSink<
                        SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportSchemaEvolutionSink {
    private CatalogTable catalogTable;
    private SelectDBConfig selectDBConfig;
    private String jobId;
    private ReadonlyConfig readonlyConfig;

    @Override
    public String getPluginName() {
        return "SelectDBCloud";
    }

    public SelectDBSink(CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
        this.selectDBConfig = SelectDBConfig.loadConfig(readonlyConfig);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SelectDBSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        SelectDBSinkWriter selectDBSinkWriter =
                new SelectDBSinkWriter(
                        context, Collections.emptyList(), catalogTable, selectDBConfig, jobId);
        selectDBSinkWriter.initializeLoad(Collections.emptyList());
        return selectDBSinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> restoreWriter(
            SinkWriter.Context context, List<SelectDBSinkState> states) throws IOException {
        SelectDBSinkWriter selectDBSinkWriter =
                new SelectDBSinkWriter(context, states, catalogTable, selectDBConfig, jobId);
        selectDBSinkWriter.initializeLoad(states);
        return selectDBSinkWriter;
    }

    @Override
    public Optional<Serializer<SelectDBSinkState>> getWriterStateSerializer() {
        return Optional.of(new SelectDBSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<SelectDBCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new SelectDBCommitter(selectDBConfig));
    }

    @Override
    public Optional<Serializer<SelectDBCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new SelectDBCommitInfoSerializer());
    }

    @Override
    public Optional<SinkAggregatedCommitter<SelectDBCommitInfo, SelectDBCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<SelectDBCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "SelectDBCloud");
        if (catalogFactory == null) {
            throw new SelectDBConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(),
                            PluginType.SINK,
                            "Cannot find SelectDBCloud catalog factory"));
        }

        Catalog catalog =
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);

        return Optional.of(new SelectDBSaveModeHandler(selectDBConfig, catalog, catalogTable));
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}
