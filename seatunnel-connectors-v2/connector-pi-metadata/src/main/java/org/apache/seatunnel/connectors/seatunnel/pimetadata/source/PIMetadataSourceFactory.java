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

package org.apache.seatunnel.connectors.seatunnel.pimetadata.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class PIMetadataSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "PI-Metadata";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // ================= Core required configuration =================
                .required(
                        PIConfig.PI_WEB_API_URL, // PI Web API server address
                        PIConfig.PI_PATHS) // PI data paths for metadata extraction

                // ================= Optional configuration =================
                .optional(PIConfig.AUTH_TYPE) // Authentication type, default Basic
                .optional(PIConfig.JSON_FIELD) // JSON field mapping
                .optional(PIConfig.SCHEMA) // User-defined Schema
                .optional(PIConfig.CONNECTION_TIMEOUT_MS) // Connection timeout
                .optional(PIConfig.READ_TIMEOUT_MS) // Read timeout
                .optional(PIConfig.RETRY_ATTEMPTS) // Retry attempts
                .optional(PIConfig.RETRY_BACKOFF_MULTIPLIER_MS) // Retry backoff multiplier
                .optional(PIConfig.WEBIDS_PER_SPLIT) // WebIDs per split for parallel processing
                .optional(PIConfig.TRUST_ALL_CERTS) // Whether to trust all SSL certificates

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
        return PIMetadataSource.class;
    }

    @Override
    public TableSource createSource(TableSourceFactoryContext context) {
        return () -> new PIMetadataSource(context.getOptions());
    }
}
