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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils;

import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.KingbaseValueConverter;
import io.debezium.connector.kingbase.connection.KingbaseConnection;

import java.nio.charset.Charset;

public class KingbaseConnectionUtils {

    /**
     * Create a new KingbaseValueConverterBuilder instance and offer type registry for JDBC
     * connection.
     *
     * <p>It is created in this package because some methods (e.g., includeUnknownDatatypes) of
     * KingbaseConnectorConfig is protected.
     */
    public static KingbaseConnection.KingbaseValueConverterBuilder newKingbaseValueConverterBuilder(
            KingbaseConnectorConfig config) {
        try (KingbaseConnection connection = new KingbaseConnection(config.getJdbcConfig())) {
            final Charset databaseCharset = connection.getDatabaseCharset();
            return (typeRegistry) ->
                    KingbaseValueConverter.of(config, databaseCharset, typeRegistry);
        }
    }
}
