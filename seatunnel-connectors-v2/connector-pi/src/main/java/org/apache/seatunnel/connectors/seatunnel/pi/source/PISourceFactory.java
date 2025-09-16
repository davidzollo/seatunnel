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

package org.apache.seatunnel.connectors.seatunnel.pi.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;

import com.google.auto.service.AutoService;

/**
 * PI Web API Batch Source Connector Factory
 *
 * <p>Responsible for creating PI Web API batch processing source connector instances.
 */
@AutoService(Factory.class)
public class PISourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "PI";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // ================= Core required configuration =================
                .required(
                        PIConfig.PI_WEB_API_URL, // PI Web API server address
                        PIConfig.PI_PATHS, // PI data paths to read
                        PIConfig.START_TIME, // Start time for batch processing
                        PIConfig.END_TIME, // End time for batch processing
                        PIConfig.SCHEMA) // User-defined Schema

                // ================= Batch mode configuration =================
                // For batch processing, read_mode defaults to BATCH and can be omitted
                //                .optional(PIConfig.READ_MODE) // Read mode: BATCH (default),
                // supports BATCH only

                // ================= JSON field mapping configuration =================
                // Made optional with default mapping for common use cases
                .optional(PIConfig.JSON_FIELD) // JSON field mapping, uses default if not specified

                // ================= Authentication configuration =================
                .optional(PIConfig.AUTH_TYPE) // Authentication type, default Basic

                // ================= SSL/HTTPS configuration =================
                .optional(PIConfig.TRUST_ALL_CERTS) // Whether to trust all SSL certificates
                .optional(PIConfig.VERIFY_HOSTNAME) // Whether to verify SSL hostname

                // ================= Basic connection configuration =================
                .optional(PIConfig.CONNECTION_TIMEOUT_MS) // Connection timeout
                .optional(PIConfig.READ_TIMEOUT_MS) // Read timeout
                .optional(PIConfig.RETRY_ATTEMPTS) // Retry attempts
                .optional(PIConfig.RETRY_BACKOFF_MULTIPLIER_MS) // Retry backoff multiplier
                .optional(PIConfig.RETRY_BACKOFF_MAX_MS) // Retry backoff maximum time

                // ================= Batch/Historical data configuration =================
                .optional(PIConfig.MAX_COUNT) // Maximum returned records per query
                .optional(PIConfig.BOUNDARY_TYPE) // Boundary type

                // ================= Batch processing performance configuration =================
                .optional(PIConfig.WEBIDS_PER_SPLIT) // Number of WebIDs per split
                .optional(PIConfig.MAX_SPLITS) // Maximum number of splits
                .optional(PIConfig.AUTO_ADJUST_SPLIT_SIZE) // Automatically adjust split size
                .optional(PIConfig.WEBID_RESOLVE_BATCH_SIZE) // WebID resolution batch size
                .optional(PIConfig.WEBID_RESOLVE_DELAY_MS) // WebID resolution delay

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
        return PISource.class;
    }

    @Override
    public <
                    T,
                    SplitT extends org.apache.seatunnel.api.source.SourceSplit,
                    StateT extends java.io.Serializable>
            org.apache.seatunnel.api.table.connector.TableSource<T, SplitT, StateT> createSource(
                    TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new PISource(context.getOptions());
    }
}
