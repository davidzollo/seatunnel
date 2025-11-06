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

package org.apache.seatunnel.connectors.cdc.pi.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;

import com.google.auto.service.AutoService;

/**
 * PI CDC Source connector Factory
 *
 * <p>Responsible for creating PI CDC Source connector instances
 */
@AutoService(Factory.class)
public class PICDCSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "PI-CDC";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // ================= Required configuration items =================
                .required(
                        PIConfig.PI_WEB_API_URL, // PI Web API server address
                        PIConfig.PI_PATHS, // PI data paths to monitor
                        PIConfig.JSON_FIELD, //  JSON field mapping
                        PIConfig.SCHEMA) // User-defined Schema

                // ================= Authentication configuration =================
                .optional(PIConfig.AUTH_TYPE) // Authentication type, default Basic
                .optional(PIConfig.RETRIEVAL_MODE) // Point-in-time retrieval mode
                // ================= SSL/HTTPS configuration =================
                //                .optional(PIConfig.TRUST_ALL_CERTS) // Whether to trust all SSL
                // certificates
                //                .optional(PIConfig.VERIFY_HOSTNAME) // Whether to verify SSL host
                // name

                // ================= Basic connection configuration =================
                .optional(PIConfig.CONNECTION_TIMEOUT_MS) // Connection timeout
                .optional(PIConfig.READ_TIMEOUT_MS) // Read timeout
                .optional(PIConfig.RETRY_ATTEMPTS) // Retry attempts
                .optional(PIConfig.RETRY_BACKOFF_MULTIPLIER_MS) // Retry backoff multiplier
                .optional(PIConfig.RETRY_BACKOFF_MAX_MS) // Retry backoff maximum time

                // ================= Real-time streaming configuration =================
                .optional(PIConfig.INCLUDE_INITIAL_VALUES) // Whether to include initial values
                .optional(PIConfig.HEARTBEAT_RATE) // Heartbeat frequency
                .optional(PIConfig.CHANNEL_POLLING_INTERVAL_MS) // Channel polling interval

                // ================= WebSocket basic configuration =================
                .optional(PIConfig.WEBSOCKET_CONNECTION_WAIT_TIMEOUT_MS) // WebSocket connection
                // timeout
                .optional(PIConfig.WEBSOCKET_MAX_RETRIES) // WebSocket maximum reconnection attempts
                .optional(PIConfig.WEBSOCKET_BUFFER_SIZE) // WebSocket message buffer size

                // ================= WebSocket advanced configuration =================
                // For expert users who need fine-grained control
                .optional(
                        PIConfig.WEBSOCKET_CONNECTION_STATUS_CHECK_INTERVAL_MS) // Connection status
                // check interval
                .optional(PIConfig.WEBSOCKET_ALLOW_BACKGROUND_CONNECTION) // Whether to allow
                // background connection

                // ================= Split configuration =================
                .optional(PIConfig.MAX_WEBIDS_PER_SPLIT) // Maximum PI Paths per split

                // ================= Authentication conditional dependencies =================
                // When auth_type=Basic, username and password are required
                .conditional(PIConfig.AUTH_TYPE, AuthType.BASIC, PIConfig.USERNAME)
                .conditional(PIConfig.AUTH_TYPE, AuthType.BASIC, PIConfig.PASSWORD)

                // When auth_type=Bearer, Bearer Token is required
                .conditional(PIConfig.AUTH_TYPE, AuthType.BEARER, PIConfig.BEARER_TOKEN)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return PICDCSource.class;
    }

    @Override
    public <
                    T,
                    SplitT extends org.apache.seatunnel.api.source.SourceSplit,
                    StateT extends java.io.Serializable>
            org.apache.seatunnel.api.table.connector.TableSource<T, SplitT, StateT> createSource(
                    TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new PICDCSource(context.getOptions());
    }
}
