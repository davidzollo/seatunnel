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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.config;

import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.option.KingbaseOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.kingbase.KingbaseConnector;
import io.debezium.connector.kingbase.KingbaseConnectorConfig;

public class KingbaseSourceConfigFactoryTest {

    @Test
    public void testCreateDebeziumConfigUsesKingbaseDefaults() {
        KingbaseSourceConfigFactory factory = createFactory();
        factory.tableList("sales.public.orders", "sales.audit.order_log");
        KingbaseSourceConfig sourceConfig = factory.create(2);

        Configuration dbzConfig = sourceConfig.getDbzConfiguration();

        Assertions.assertEquals("com.kingbase8.Driver", sourceConfig.getDriverClassName());
        Assertions.assertEquals(
                KingbaseConnector.class.getCanonicalName(), dbzConfig.getString("connector.class"));
        Assertions.assertEquals("localhost", dbzConfig.getString("database.hostname"));
        Assertions.assertEquals("sales", dbzConfig.getString("database.dbname"));
        Assertions.assertEquals(
                KingbaseOptions.DECODING_PLUGIN_NAME.defaultValue(),
                dbzConfig.getString("plugin.name"));
        KingbaseConnectorConfig connectorConfig = new KingbaseConnectorConfig(dbzConfig);
        Assertions.assertEquals(
                "syslogical_output", connectorConfig.plugin().getPostgresPluginName());
        Assertions.assertEquals(
                KingbaseOptions.SLOT_NAME.defaultValue(), dbzConfig.getString("slot.name"));
        Assertions.assertEquals(
                "public.orders,audit.order_log", dbzConfig.getString("table.include.list"));
        Assertions.assertEquals(
                EmbeddedDatabaseHistory.class.getCanonicalName(),
                dbzConfig.getString("database.history"));
        Assertions.assertEquals(
                "true", dbzConfig.getString("database.history.skip.unparseable.ddl"));
        Assertions.assertEquals("disable", dbzConfig.getString("database.sslmode"));
        Assertions.assertEquals("false", dbzConfig.getString("tombstones.on.delete"));
    }

    @Test
    public void testExplicitDecoderbufsKeepsServerPluginName() {
        KingbaseSourceConfigFactory factory = createFactory();
        factory.debeziumProperties(
                Configuration.create().with("plugin.name", "decoderbufs").build().asProperties());

        Configuration dbzConfig = factory.create(0).getDbzConfiguration();
        KingbaseConnectorConfig connectorConfig = new KingbaseConnectorConfig(dbzConfig);

        Assertions.assertEquals("decoderbufs", dbzConfig.getString("plugin.name"));
        Assertions.assertEquals("decoderbufs", connectorConfig.plugin().getPostgresPluginName());
    }

    @Test
    public void testOriginUrlQueryPropertiesAreVisibleToDebeziumJdbcConnections() {
        KingbaseSourceConfigFactory factory = createFactory();
        factory.originUrl(
                "jdbc:kingbase8://localhost:54321/sales?sslmode=require&ApplicationName=wt-ui");

        Configuration dbzConfig = factory.create(0).getDbzConfiguration();

        Assertions.assertEquals("require", dbzConfig.getString("database.sslmode"));
        Assertions.assertEquals("wt-ui", dbzConfig.getString("database.ApplicationName"));
    }

    private KingbaseSourceConfigFactory createFactory() {
        KingbaseSourceConfigFactory factory = new KingbaseSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(54321);
        factory.username("kingbase_user");
        factory.password("kingbase_pass");
        factory.databaseList("sales");
        factory.originUrl("jdbc:kingbase8://localhost:54321/sales");
        factory.startupOptions(new StartupConfig(StartupMode.INITIAL, null, null, null));
        factory.stopOptions(new StopConfig(StopMode.NEVER, null, null, null));
        return factory;
    }
}
