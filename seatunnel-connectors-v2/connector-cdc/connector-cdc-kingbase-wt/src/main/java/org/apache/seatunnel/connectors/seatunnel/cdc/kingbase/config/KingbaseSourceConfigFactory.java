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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.option.KingbaseOptions;

import org.apache.commons.lang3.StringUtils;

import io.debezium.connector.kingbase.KingbaseConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class KingbaseSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "kingbase_cdc_source";

    private static final String DRIVER_CLASS_NAME = "com.kingbase8.Driver";
    private static final boolean SUPPORTED_EXACTLY_ONCE = false;

    private String decodingPluginName = KingbaseOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = KingbaseOptions.SLOT_NAME.defaultValue();

    @Override
    public JdbcSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(KingbaseOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(KingbaseOptions.SLOT_NAME);
        return this;
    }

    @Override
    public KingbaseSourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", KingbaseConnector.class.getCanonicalName());
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular Kingbase
        // database server/cluster being monitored. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names coming
        // from this connector. Only alphanumeric characters and underscores should be used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        // Debezium's nested JDBC connections must reuse the exact JDBC URL so host-side TCP
        // bridges, sslmode and other transport flags stay identical to the outer catalog path.
        props.setProperty("database.url", checkNotNull(originUrl));
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("plugin.name", decodingPluginName);
        props.setProperty("slot.name", slotName);
        props.setProperty(
                "publication.autocreate.mode",
                PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        // Debezium still needs an in-memory history store for table schema snapshots in
        // PostgreSQL-compatible connectors. It is not a complete DDL replay mechanism.
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        props.setProperty("include.schema.changes", String.valueOf(false));
        // tombstones.on.delete is set to false to avoid tombstones being sent to the sink
        props.setProperty("tombstones.on.delete", String.valueOf(false));
        // Debezium only passes explicit database.* entries to its secondary JDBC connections.
        // Without an explicit sslmode, the Kingbase driver may still attempt SSL negotiation and
        // fail against plain-text local/container deployments during split generation.
        props.setProperty("database.sslmode", "disable");

        if (tableList != null) {
            // Debezium Kingbase follows PostgreSQL table filters, so table.include.list uses
            // schemaName.tableName and drops the SeaTunnel database prefix.
            String tableIncludeList =
                    tableList.stream()
                            .map(table -> table.substring(table.indexOf(".") + 1))
                            .collect(Collectors.joining(","));
            props.setProperty("table.include.list", tableIncludeList);
        }

        String colIncludeRegex =
                buildColumnIncludeList(readColumnsMap, databaseList, tableList, true);
        if (StringUtils.isNotBlank(colIncludeRegex)) {
            props.setProperty("column.include.list", colIncludeRegex);
        }

        propagateJdbcQueryProperties(props);

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        KingbaseSourceConfig config =
                new KingbaseSourceConfig(
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
                        SUPPORTED_EXACTLY_ONCE,
                        whereCondition,
                        readColumnsMap);
        config.setEnableConcurrentRead(enableConcurrentRead);
        return config;
    }

    /**
     * Mirror JDBC URL query parameters into Debezium's database.* namespace so every secondary
     * Kingbase JDBC connection sees the same transport flags as the outer SeaTunnel catalog
     * connection.
     */
    private void propagateJdbcQueryProperties(Properties props) {
        if (StringUtils.isBlank(originUrl)) {
            return;
        }

        int queryStartIndex = originUrl.indexOf('?');
        if (queryStartIndex < 0 || queryStartIndex == originUrl.length() - 1) {
            return;
        }

        String queryString = originUrl.substring(queryStartIndex + 1);
        for (String entry : queryString.split("&")) {
            if (StringUtils.isBlank(entry)) {
                continue;
            }

            int separatorIndex = entry.indexOf('=');
            String key =
                    separatorIndex >= 0
                            ? decodeJdbcQueryToken(entry.substring(0, separatorIndex))
                            : decodeJdbcQueryToken(entry);
            if (StringUtils.isBlank(key)) {
                continue;
            }

            String value =
                    separatorIndex >= 0
                            ? decodeJdbcQueryToken(entry.substring(separatorIndex + 1))
                            : "";
            props.setProperty("database." + key, value);
        }
    }

    /**
     * Decode URL-encoded JDBC query tokens so values such as application names stay intact when
     * forwarded into Debezium JDBC properties.
     */
    private String decodeJdbcQueryToken(String token) {
        try {
            return URLDecoder.decode(token, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 must be supported by the current JVM", e);
        }
    }
}
