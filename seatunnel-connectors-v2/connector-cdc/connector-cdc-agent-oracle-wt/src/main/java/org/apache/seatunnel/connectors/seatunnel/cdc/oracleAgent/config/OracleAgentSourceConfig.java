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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import io.debezium.connector.oracle.OracleAgentConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import lombok.Getter;

import java.util.List;
import java.util.Properties;

public class OracleAgentSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    @Getter private final String oracleAgentHost;
    @Getter private final Integer oracleAgentPort;

    public OracleAgentSourceConfig(
            StartupConfig startupConfig,
            StopConfig stopConfig,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            int sampleShardingThreshold,
            int inverseSamplingRate,
            boolean enableHashSplitterForStringColumn,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String oracle9BridgeHost,
            Integer oracle9BridgePort,
            String username,
            String password,
            String originUrl,
            int fetchSize,
            String serverTimeZone,
            long connectTimeoutMillis,
            int connectMaxRetries,
            int connectionPoolSize,
            boolean exactlyOnce,
            String whereCondition) {
        super(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                enableHashSplitterForStringColumn,
                dbzProperties,
                driverClassName,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce,
                whereCondition);
        this.oracleAgentHost = oracle9BridgeHost;
        this.oracleAgentPort = oracle9BridgePort;
    }

    @Override
    public OracleAgentConnectorConfig getDbzConnectorConfig() {
        return new OracleAgentConnectorConfig(getDbzConfiguration());
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }
}
