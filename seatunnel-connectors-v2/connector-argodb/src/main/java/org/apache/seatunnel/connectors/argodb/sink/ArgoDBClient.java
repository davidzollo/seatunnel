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

import org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig;

import io.transwarp.holodesk.sink.ArgoDBConfig;
import io.transwarp.holodesk.sink.ArgoDBSinkClient;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.Closeable;

@Getter
public class ArgoDBClient implements Closeable {

    private final ArgoDBSinkClient client;

    @SneakyThrows
    public ArgoDBClient(ArgoDBSinkConfig config) {
        Class.forName("io.transwarp.jdbc.InceptorDriver");
        ArgoDBConfig argoDBConfig =
                ArgoDBConfig.builder()
                        .url(config.getUrl())
                        .user(config.getUser())
                        .passwd(config.getPassword())
                        .build();
        io.transwarp.holodesk.sink.ArgoDBSinkConfig argoDBSinkConfig =
                io.transwarp.holodesk.sink.ArgoDBSinkConfig.builder()
                        .argoConfig(argoDBConfig)
                        .tmpDirectory(config.getTmpDirectory())
                        .build();
        this.client = new ArgoDBSinkClient(argoDBSinkConfig);
        client.init();
    }

    @SneakyThrows
    public synchronized void openTable(String tablePath) {
        if (!client.isTableOpen(tablePath)) {
            client.openTable(tablePath);
        }
    }

    @SneakyThrows
    @Override
    public void close() {
        client.close();
    }
}
