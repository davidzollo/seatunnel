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

package io.debezium.connector.kingbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

import java.util.Properties;

public class KingbaseConnectorConfigTest {

    @Test
    public void testReplicationConnectionPropertiesRetainJdbcTransportFlags() {
        KingbaseConnectorConfig connectorConfig =
                new KingbaseConnectorConfig(
                        Configuration.create()
                                .with("database.server.name", "kingbase_test")
                                .with("database.hostname", "127.0.0.1")
                                .with("database.port", "54321")
                                .with("database.user", "system")
                                .with("database.password", "kingbase")
                                .with("database.dbname", "test")
                                .with("database.sslmode", "disable")
                                .with("database.ApplicationName", "wt-ui")
                                .with("plugin.name", "pgoutput")
                                .with("slot.name", "seatunnel_slot")
                                .build());

        Properties properties = connectorConfig.replicationConnectionProperties();

        Assertions.assertEquals("system", properties.getProperty("user"));
        Assertions.assertEquals("kingbase", properties.getProperty("password"));
        Assertions.assertEquals("disable", properties.getProperty("sslmode"));
        Assertions.assertEquals("wt-ui", properties.getProperty("ApplicationName"));
        Assertions.assertEquals("9.4", properties.getProperty("assumeMinServerVersion"));
        Assertions.assertEquals("database", properties.getProperty("replication"));
        Assertions.assertEquals("simple", properties.getProperty("preferQueryMode"));
    }
}
