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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.connectors.cdc.pi.serialization.PICDCCheckpointStateSerializer;
import org.apache.seatunnel.connectors.cdc.pi.serialization.PICDCSplitSerializer;
import org.apache.seatunnel.connectors.cdc.pi.source.reader.PICDCSourceReader;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCCheckpointState;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCSplit;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PISchemaBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * PI CDC source connector
 *
 * <p>Split from PISource for real-time functionality, specifically for real-time data capture from
 * PI system
 */
public class PICDCSource
        implements SeaTunnelSource<SeaTunnelRow, PICDCSplit, PICDCCheckpointState>,
                SupportParallelism {

    private static final Logger log = LoggerFactory.getLogger(PICDCSource.class);

    public static final String PLUGIN_NAME = "PI-CDC";

    private SeaTunnelRowType rowType;
    private PIConfigHelper configHelper;
    private CatalogTable catalogTable;

    /** Default constructor */
    public PICDCSource() {}

    /** Constructor with configuration, used for Factory creation */
    public PICDCSource(ReadonlyConfig config) {
        try {
            prepare(config);
        } catch (Exception e) {
            throw new PIConnectorException(
                    PIErrorCode.INITIALIZATION_FAILED,
                    "PI CDC source connector construction failed",
                    e);
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    public void prepare(ReadonlyConfig config) throws Exception {

        try {
            // Reuse connector-pi's connection configuration
            this.configHelper = new PIConfigHelper(config);

            // Validate configuration
            CheckResult checkResult =
                    CheckConfigUtil.checkAllExists(
                            config.toConfig(),
                            PIConfig.PI_WEB_API_URL.key(),
                            PIConfig.USERNAME.key(),
                            PIConfig.PASSWORD.key(),
                            PIConfig.PI_PATHS.key());

            if (!checkResult.isSuccess()) {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_VALIDATION_FAILED,
                        "PI CDC source configuration validation failed: " + checkResult.getMsg());
            }

            // Reuse connector-pi's Schema builder
            if (config.getOptional(PIConfig.SCHEMA).isPresent()) {
                // User-defined Schema - fully generic
                this.rowType = PISchemaBuilder.createRowTypeFromUserSchema(config);

            } else {
                // Use default Schema (backward compatibility)
                this.rowType = PISchemaBuilder.createDefaultRowType();
            }

            // Validate Schema
            PISchemaBuilder.validateSchema(this.rowType);

            // Create CatalogTable (reuse PIMetadataSource logic)
            if (config.getOptional(PIConfig.SCHEMA).isPresent()) {
                this.catalogTable = CatalogTableUtil.buildWithConfig(config);
            } else {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_INVALID,
                        "PI CDC schema configuration not exists, please check your task configuration.");
            }

        } catch (Exception e) {
            log.error("PI CDC source connector initialization failed", e);
            if (e instanceof PIConnectorException) {
                throw e;
            }
            throw new PIConnectorException(
                    PIErrorCode.INITIALIZATION_FAILED,
                    "PI CDC source connector initialization failed",
                    e);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        // PI CDC is always unbounded (real-time stream)
        return Boundedness.UNBOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return rowType;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, PICDCSplit> createReader(SourceReader.Context readerContext)
            throws Exception {

        return new PICDCSourceReader(configHelper, rowType, readerContext);
    }

    @Override
    public SourceSplitEnumerator<PICDCSplit, PICDCCheckpointState> createEnumerator(
            SourceSplitEnumerator.Context<PICDCSplit> enumeratorContext) throws Exception {

        return new PICDCSplitEnumerator(configHelper, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<PICDCSplit, PICDCCheckpointState> restoreEnumerator(
            SourceSplitEnumerator.Context<PICDCSplit> enumeratorContext,
            PICDCCheckpointState checkpointState)
            throws Exception {
        log.info("Restore PI CDC split enumerator");
        return new PICDCSplitEnumerator(configHelper, enumeratorContext, checkpointState);
    }

    @Override
    public Serializer<PICDCSplit> getSplitSerializer() {
        return new PICDCSplitSerializer();
    }

    @Override
    public Serializer<PICDCCheckpointState> getEnumeratorStateSerializer() {
        return new PICDCCheckpointStateSerializer();
    }

    public CatalogTable getProducedCatalogTable() {
        return catalogTable;
    }
}
