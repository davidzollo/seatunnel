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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3ConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift.RedshiftCatalog;
import org.apache.seatunnel.connectors.seatunnel.redshift.commit.S3RedshiftSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class S3RedshiftSink extends BaseHdfsFileSink
        implements SupportMultiTableSink, SupportSaveMode, SupportSchemaEvolutionSink {

    private S3RedshiftConf s3RedshiftConf;
    private CatalogTable catalogTable;
    private ReadonlyConfig readonlyConfig;

    public S3RedshiftSink(
            CatalogTable catalogTable,
            S3RedshiftConf s3RedshiftConf,
            Config pluginConfig,
            ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.pluginConfig = pluginConfig;
        this.hadoopConf =
                S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromConfig(this.pluginConfig));
        this.s3RedshiftConf = s3RedshiftConf;
        this.catalogTable = catalogTable;
        this.setTypeInfo(catalogTable.getTableSchema().toPhysicalRowDataType());
    }

    @Override
    public String getPluginName() {
        return "S3Redshift";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        S3ConfigOptions.S3_BUCKET.key(),
                        S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                        S3RedshiftConfig.JDBC_URL.key(),
                        S3RedshiftConfig.JDBC_USER.key(),
                        S3RedshiftConfig.JDBC_PASSWORD.key());
        if (!checkResult.isSuccess()) {
            throw new S3RedshiftConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, checkResult.getMsg()));
        }
        this.pluginConfig = this.pluginConfig;
        hadoopConf =
                S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromConfig(this.pluginConfig));
        s3RedshiftConf = S3RedshiftConf.valueOf(this.pluginConfig);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(
                new S3RedshiftSinkAggregatedCommitter(
                        hadoopConf, s3RedshiftConf, seaTunnelRowType));
    }

    @Override
    public S3RedshiftChangelogWriter createWriter(SinkWriter.Context context) {
        return new S3RedshiftChangelogWriter(
                newWriteStrategy(),
                hadoopConf,
                context,
                jobId,
                Collections.emptyList(),
                seaTunnelRowType,
                s3RedshiftConf);
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new S3RedshiftChangelogWriter(
                newWriteStrategy(),
                hadoopConf,
                context,
                jobId,
                states,
                seaTunnelRowType,
                s3RedshiftConf);
    }

    @Override
    public Optional<SinkCommitter<FileCommitInfo>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        S3RedshiftSQLGenerator sqlGenerator;
        if (catalogTable != null) {
            sqlGenerator = new S3RedshiftSQLGenerator(s3RedshiftConf, catalogTable);
        } else {
            sqlGenerator = new S3RedshiftSQLGenerator(s3RedshiftConf, seaTunnelRowType);
        }
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo(readonlyConfig.get(S3RedshiftConfig.JDBC_URL));
        RedshiftCatalog catalog =
                new RedshiftCatalog(
                        "Redshift",
                        readonlyConfig.get(S3RedshiftConfig.JDBC_USER),
                        readonlyConfig.get(S3RedshiftConfig.JDBC_PASSWORD),
                        urlInfo,
                        readonlyConfig.get(JdbcCatalogOptions.SCHEMA));
        return Optional.of(
                new S3RedshiftSaveModeHandler(
                        s3RedshiftConf.getSchemaSaveMode(),
                        s3RedshiftConf.getDataSaveMode(),
                        catalog,
                        catalogTable,
                        s3RedshiftConf.getCustomSql(),
                        sqlGenerator,
                        s3RedshiftConf));
    }

    private WriteStrategy newWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        return writeStrategy;
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.DROP_COLUMN);
    }
}
