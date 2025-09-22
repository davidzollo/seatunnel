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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pi.serialization.PICheckpointStateSerializer;
import org.apache.seatunnel.connectors.seatunnel.pi.serialization.PISplitSerializer;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PICheckpointState;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PISplit;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PISplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PISchemaBuilder;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * PI Web API Source Connector
 *
 * <p>Supports real-time streaming and batch processing modes
 *
 * <p>Fully generic design supporting user-defined Schema and all PI data types
 *
 * <p>Supports SeaTunnel's parallel processing and checkpoint mechanisms
 */
@Slf4j
public class PISource
        implements SeaTunnelSource<SeaTunnelRow, PISplit, PICheckpointState>,
                SupportParallelism,
                SupportColumnProjection {

    public static final String PLUGIN_NAME = "PI";

    private SeaTunnelRowType rowType;
    private PIConfigHelper configHelper;
    private CatalogTable catalogTable;

    /**
     * Constructor - called by PISourceFactory
     *
     * @param config
     */
    public PISource(ReadonlyConfig config) {
        try {
            prepare(config);
        } catch (Exception e) {
            log.error("PI Source initialization failed", e);
            throw new PIConnectorException(
                    PIErrorCode.INITIALIZATION_FAILED, "PI Source initialization failed", e);
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    /**
     * Initialize the connector
     *
     * @param config SeaTunnel configuration options
     * @throws Exception thrown when initialization fails
     */
    public void prepare(ReadonlyConfig config) throws Exception {
        log.info("Initializing PI Source connector");

        // Create configuration helper
        this.configHelper = new PIConfigHelper(config);

        // Validate configuration
        validateConfig(configHelper);

        // Create or parse user Schema
        if (config.getOptional(PIConfig.SCHEMA).isPresent()) {
            // User-defined Schema - fully generic
            this.rowType = PISchemaBuilder.createRowTypeFromUserSchema(config);

        } else {
            // Use default Schema (backward compatibility)
            this.rowType = PISchemaBuilder.createDefaultRowType();
        }

        // Validate Schema
        PISchemaBuilder.validateSchema(this.rowType);

        // Create CatalogTable
        if (config.getOptional(PIConfig.SCHEMA).isPresent()) {
            this.catalogTable = CatalogTableUtil.buildWithConfig(config);
        } else {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID,
                    "Schema configuration not exists, please check your task configuration.");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return rowType;
    }

    @Override
    public SourceSplitEnumerator<PISplit, PICheckpointState> createEnumerator(
            SourceSplitEnumerator.Context<PISplit> enumeratorContext) {

        return new PISplitEnumerator(configHelper, enumeratorContext);
    }

    @Override
    public SourceReader<SeaTunnelRow, PISplit> createReader(SourceReader.Context readerContext) {

        return new PISourceReader(configHelper, rowType, readerContext);
    }

    @Override
    public SourceSplitEnumerator<PISplit, PICheckpointState> restoreEnumerator(
            SourceSplitEnumerator.Context<PISplit> enumeratorContext,
            PICheckpointState checkpointState) {
        log.info("Restoring PI Source Split Enumerator from checkpoint");
        return new PISplitEnumerator(configHelper, enumeratorContext, checkpointState);
    }

    public CatalogTable getProducedCatalogTable() {
        return catalogTable;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public Serializer<PISplit> getSplitSerializer() {
        return new PISplitSerializer();
    }

    @Override
    public Serializer<PICheckpointState> getEnumeratorStateSerializer() {
        log.info("Creating PI Source Enumerator State Serializer");
        return new PICheckpointStateSerializer();
    }

    /**
     * Validate connector configuration
     *
     * @param config
     */
    private void validateConfig(PIConfigHelper config) {
        // Validate required configuration items
        if (config.getServerUrl() == null || config.getServerUrl().trim().isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_MISSING_URL, "PI Web API URL not configured");
        }

        // Validate authentication information
        AuthType authType = config.getAuthType();
        if (AuthType.BASIC.equals(authType)) {
            if (config.getUsername() == null || config.getPassword() == null) {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_MISSING_CREDENTIALS,
                        "Username and password must be configured in Basic authentication mode");
            }
        } else if (AuthType.BEARER.equals(authType)) {
            if (config.getBearerToken() == null) {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_MISSING_CREDENTIALS,
                        "Bearer Token must be configured in Bearer authentication mode");
            }
        }

        // Validate data source configuration
        if (config.getPiPaths() == null) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_MISSING_TAG_PATHS, "Must configure pi_paths");
        }

        // Validate data source quantity limit (Note: this is the number of configuration items,
        // actual WebID count may differ after resolution)
        int configuredItemCount = getConfiguredItemCount();
        if (configuredItemCount > PIConfig.MAX_SUPPORTED_WEBIDS) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_TOO_MANY_WEBIDS,
                    String.format(
                            "Configuration item count (%d) exceeds maximum supported count (%d), actual WebID count will be determined after resolution",
                            configuredItemCount, PIConfig.MAX_SUPPORTED_WEBIDS));
        }

        // Validate time range for batch mode
        if (config.getStartTime() == null) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID_TIME_RANGE,
                    "start_time must be configured in batch processing mode");
        }
    }

    /** Get total count of configured items (PI Paths or WebIDs) */
    private int getConfiguredItemCount() {
        if (configHelper.getPiPaths() != null) {
            return configHelper.getPiPaths().size();
        }
        return 0;
    }
}
