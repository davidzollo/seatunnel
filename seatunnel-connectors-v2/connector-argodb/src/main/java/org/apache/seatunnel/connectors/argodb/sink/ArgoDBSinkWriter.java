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

package org.apache.seatunnel.connectors.argodb.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig;
import org.apache.seatunnel.connectors.argodb.serialize.ArgoDBSerializer;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import io.transwarp.holodesk.sink.ArgoDBRow;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ArgoDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<ArgoDBClient> {

    private ArgoDBSinkConfig config;
    private CatalogTable table;
    private ArgoDBClient argoDBClient;
    private ArgoDBSerializer serializer;
    private AtomicInteger batchCounter = new AtomicInteger(0);

    public ArgoDBSinkWriter(ArgoDBSinkConfig config, CatalogTable table) {
        this.config = config;
        this.table = table;
        this.serializer = new ArgoDBSerializer(table);
    }

    @Override
    public Optional<Integer> primaryKey() {
        PrimaryKey primaryKey = table.getTableSchema().getPrimaryKey();
        if (primaryKey != null) {
            int index = table.getSeaTunnelRowType().indexOf(primaryKey.getColumnNames().get(0));
            return Optional.of(index);
        }
        return Optional.empty();
    }

    @Override
    public MultiTableResourceManager<ArgoDBClient> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return new ArgoDBResourceManager(new ArgoDBClient(config));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ArgoDBClient> resourceManager, int queueIndex) {
        this.argoDBClient = resourceManager.getSharedResource().get();
        argoDBClient.openTable(config.getTablePath());
    }

    @SneakyThrows
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        ArgoDBRow argodbRow = serializer.serialize(element);
        switch (element.getRowKind()) {
            case INSERT:
                if (!config.isEnableUpsertDelete()) {
                    argoDBClient.getClient().insert(config.getTablePath(), argodbRow);
                } else {
                    argoDBClient.getClient().upsert(config.getTablePath(), argodbRow);
                }
                break;
            case UPDATE_BEFORE:
                if (!config.isEnableUpsertDelete()) {
                    throw new UnsupportedEncodingException(
                            "Please enable upsert delete mode. example: enable_upsert_delete = true");
                }
                break;
            case UPDATE_AFTER:
                if (!config.isEnableUpsertDelete()) {
                    throw new UnsupportedEncodingException(
                            "Please enable upsert delete mode. example: enable_upsert_delete = true");
                } else {
                    argoDBClient.getClient().upsert(config.getTablePath(), argodbRow);
                }
                break;
            case DELETE:
                if (!config.isEnableUpsertDelete()) {
                    throw new UnsupportedEncodingException(
                            "Please enable upsert delete mode. example: enable_upsert_delete = true");
                } else {
                    argoDBClient.getClient().delete(config.getTablePath(), argodbRow);
                }
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported row kind: %s, data: %s",
                                element.getRowKind(), element));
        }

        if (batchCounter.incrementAndGet() >= config.getBatchSize()) {
            argoDBClient.getClient().flush(config.getTablePath());
            batchCounter.set(0);
        }
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        argoDBClient.getClient().flush(config.getTablePath());
        batchCounter.set(0);
        return Optional.empty();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        argoDBClient.getClient().abortTransaction(config.getTablePath());
        batchCounter.set(0);
    }

    @SneakyThrows
    @Override
    public void close() {
        argoDBClient.getClient().closeTable(config.getTablePath());
    }
}
