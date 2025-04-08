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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.DATABASE;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.ENABLE_UPSERT_DELETE;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.PASSWORD;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.TABLE;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.TMP_DIRECTORY;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.URL;
import static org.apache.seatunnel.connectors.argodb.config.ArgoDBSinkConfig.USER;

@AutoService(Factory.class)
public class ArgoDBSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return ArgoDBSinkConfig.PLUGIN_IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DATABASE, TABLE)
                .optional(USER, PASSWORD)
                .optional(TMP_DIRECTORY, BATCH_SIZE, ENABLE_UPSERT_DELETE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new ArgoDBSink(ArgoDBSinkConfig.fromConfig(config), catalogTable);
    }
}
