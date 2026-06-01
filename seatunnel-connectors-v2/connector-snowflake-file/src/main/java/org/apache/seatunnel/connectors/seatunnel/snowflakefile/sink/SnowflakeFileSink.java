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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog.SnowflakeFileCatalog;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig.LocalStageType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig.StagingBackend;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.savemode.SnowflakeFileSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileSinkState;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class SnowflakeFileSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        SnowflakeFileSinkState,
                        SnowflakeFileCommitInfo,
                        SnowflakeFileAggCommitInfo>,
                SupportSaveMode {

    private SnowflakeFileConfig config;
    private CatalogTable catalogTable;

    @Override
    public String getPluginName() {
        return "SnowflakeFile";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        // Set default values
        Map<String, Object> defaultConfigs =
                ImmutableMap.<String, Object>builder()
                        .put("staging_backend", StagingBackend.S3.name())
                        .put("s3_region", "us-east-1")
                        .put("s3_key_prefix", "snowflake-staging/")
                        .put("s3_protocol", "s3")
                        .put("local_stage_type", LocalStageType.USER.name())
                        .put("local_stage_prefix", "seatunnel-local")
                        .put("file_format", "CSV")
                        .put("field_delimiter", ",")
                        .put("record_delimiter", "\n")
                        .put("file_extension", ".csv")
                        .put("buffer_size", 1048576) // 1MB
                        .put("max_file_size", 104857600) // 100MB
                        .put("purge_after_copy", true)
                        .put("time_format", "HH24:MI:SS")
                        .put("date_format", "YYYY-MM-DD")
                        .put("timestamp_format", "YYYY-MM-DD HH24:MI:SS.FF3")
                        .build();

        pluginConfig = pluginConfig.withFallback(ConfigFactory.parseMap(defaultConfigs));

        CheckResult commonCheck =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, "account", "database", "schema", "table", "user", "password");

        if (!commonCheck.isSuccess()) {
            throw prepareFail(commonCheck.getMsg());
        }

        StagingBackend stagingBackend =
                StagingBackend.valueOf(pluginConfig.getString("staging_backend"));
        if (stagingBackend == StagingBackend.S3) {
            CheckResult s3Check =
                    CheckConfigUtil.checkAllExists(
                            pluginConfig,
                            "s3_bucket",
                            "aws_access_key_id",
                            "aws_secret_access_key");
            if (!s3Check.isSuccess()) {
                throw prepareFail(s3Check.getMsg());
            }
        } else {
            LocalStageType localStageType =
                    LocalStageType.valueOf(pluginConfig.getString("local_stage_type"));
            if (localStageType == LocalStageType.NAMED) {
                CheckResult namedStageCheck =
                        CheckConfigUtil.checkAllExists(pluginConfig, "local_stage_name");
                if (!namedStageCheck.isSuccess()) {
                    throw prepareFail(namedStageCheck.getMsg());
                }
            }
        }

        // Create config object
        this.config = new SnowflakeFileConfig(pluginConfig);
    }

    private PrepareFailException prepareFail(String message) {
        return new PrepareFailException(
                getPluginName(),
                PluginType.SINK,
                String.format(
                        "PluginName: %s, PluginType: %s, Message: %s",
                        getPluginName(), PluginType.SINK, message));
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        config.setSeaTunnelRowType(seaTunnelRowType);
        // Keep CatalogTable for save mode handling
        if (catalogTable == null) {
            List<Column> columns = new ArrayList<>();
            String[] fieldNames = seaTunnelRowType.getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                columns.add(
                        PhysicalColumn.of(
                                fieldNames[i],
                                seaTunnelRowType.getFieldType(i),
                                null, // columnLength
                                null, // scale
                                true, // nullable
                                null, // defaultValue
                                null // comment
                                ));
            }

            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    config.getDatabase(), config.getSchema(), config.getTable()),
                            TableSchema.builder().columns(columns).build(),
                            new HashMap<>(),
                            new ArrayList<>(),
                            "");
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, SnowflakeFileCommitInfo, SnowflakeFileSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new SnowflakeFileSinkWriter(config, config.getSeaTunnelRowType(), context);
    }

    @Override
    public SinkWriter<SeaTunnelRow, SnowflakeFileCommitInfo, SnowflakeFileSinkState> restoreWriter(
            SinkWriter.Context context, List<SnowflakeFileSinkState> states) throws IOException {
        return new SnowflakeFileSinkWriter(config, config.getSeaTunnelRowType(), context, states);
    }

    @Override
    public Optional<Serializer<SnowflakeFileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<SnowflakeFileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<SnowflakeFileCommitInfo, SnowflakeFileAggCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new SnowflakeFileSinkAggCommitter(config));
    }

    @Override
    public Optional<Serializer<SnowflakeFileAggCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable != null) {
            // Create Snowflake catalog
            SnowflakeFileCatalog catalog = new SnowflakeFileCatalog(config);
            TablePath tablePath =
                    TablePath.of(config.getDatabase(), config.getSchema(), config.getTable());

            return Optional.of(
                    new SnowflakeFileSaveModeHandler(
                            config.getSchemaSaveMode(),
                            config.getDataSaveMode(),
                            catalog,
                            tablePath,
                            catalogTable,
                            null, // tempTablePath
                            null, // tempCatalogTable
                            config.getCustomSql()));
        }
        return Optional.empty();
    }
}
