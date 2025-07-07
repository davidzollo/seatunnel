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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.connector.oracle.OracleConnector;

import java.sql.DriverManager;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/** A factory to initialize {@link OracleSourceConfig}. */
public class OracleSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;
    private static final String DATABASE_SERVER_NAME = "oracle_logminer";

    private static final String DRIVER_CLASS_NAME = "oracle.jdbc.OracleDriver";
    public static final String SCHEMA_CHANGE_KEY = "include.schema.changes";
    public static final Boolean SCHEMA_CHANGE_DEFAULT = true;
    public static final String LOG_MINING_STRATEGY_KEY = "log.mining.strategy";
    public static final String LOG_MINING_STRATEGY_DEFAULT = "online_catalog";

    /** Creates a new {@link OracleSourceConfig} for the given subtask {@code subtaskId}. */
    public OracleSourceConfig create(int subtask) {
        validateConfig();

        try {
            // Load DriverManager first to avoid deadlock between DriverManager's
            // static initialization block and specific driver class's static
            // initialization block.
            DriverManager.getDrivers();

            Class.forName(DRIVER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Properties props = new Properties();
        props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
        // Logical name that identifies and provides a namespace for the particular Oracle
        // database server being
        // monitored. The logical name should be unique across all other connectors, since it is
        // used as a prefix
        // for all Kafka topic names emanating from this connector. Only alphanumeric characters
        // and
        // underscores should be used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.url", checkNotNull(originUrl));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        // Some scenarios do not require automatic capture of table structure changes, so the
        // default setting is true.
        props.setProperty(SCHEMA_CHANGE_KEY, String.valueOf(schemaChangeEnabled));
        props.setProperty(
                LOG_MINING_STRATEGY_KEY,
                schemaChangeEnabled ? "redo_log_catalog" : LOG_MINING_STRATEGY_DEFAULT);

        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeoutMillis));
        // tombstones.on.delete is set to false to avoid tombstones being sent to the sink
        props.setProperty("tombstones.on.delete", String.valueOf(false));

        // If the maximum value is not set, logminer may fail to capture data
        props.setProperty("log.mining.batch.size.max", String.valueOf(2147483646));
        props.setProperty("log.mining.batch.size.min", String.valueOf(2000));

        // Optimize logminer latency
        props.setProperty("log.mining.strategy", "online_catalog");
        props.setProperty("log.mining.continuous.mine", String.valueOf(true));

        if (tableList != null) {
            // Oracle identifier is of the form schemaName.tableName
            props.setProperty(
                    "table.include.list",
                    tableList.stream()
                            .map(
                                    tableStr -> {
                                        String[] splits = tableStr.split("\\.");
                                        if (splits.length == 2) {
                                            return tableStr;
                                        }
                                        if (splits.length == 3) {
                                            return String.join(".", splits[1], splits[2]);
                                        }
                                        throw new IllegalArgumentException(
                                                "Invalid table name: " + tableStr);
                                    })
                            .collect(Collectors.joining(",")));
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            String debeziumSchemaChanges =
                    dbzProperties.getProperty(
                            SCHEMA_CHANGE_KEY, String.valueOf(schemaChangeEnabled));
            String debeziumLogMiningStrategy = dbzProperties.getProperty(LOG_MINING_STRATEGY_KEY);
            if (Boolean.parseBoolean(debeziumSchemaChanges)
                    && LOG_MINING_STRATEGY_DEFAULT.equals(debeziumLogMiningStrategy)) {
                throw new IllegalArgumentException(
                        "Debezium log mining strategy must be set to redo_log_catalog when schema changes are enabled");
            }
            props.putAll(dbzProperties);
        }

        return new OracleSourceConfig(
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
                props,
                DRIVER_CLASS_NAME,
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
    }

    private void validateConfig() throws IllegalArgumentException {
        if (databaseList.size() != 1) {
            throw new IllegalArgumentException(
                    "Oracle only supports single database, databaseList: " + databaseList);
        }
    }
}
