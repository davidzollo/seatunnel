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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.commons.collections4.MapUtils;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class JdbcSinkWriter extends AbstractJdbcSinkWriter<ConnectionPoolManager>
        implements SupportSchemaEvolutionSinkWriter {
    private JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;
    private final JdbcDialect dialect;
    private final TablePath sinkTablePath;
    private final TableSchema tableSchema;
    private final TableSchema databaseTableSchema;
    private JdbcConnectionProvider connectionProvider;
    private transient boolean isOpen;
    private final Integer primaryKeyIndex;
    private final JdbcSinkConfig jdbcSinkConfig;
    private SeaTunnelRowType rowType;

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            Integer primaryKeyIndex) {
        this.sinkTablePath = sinkTablePath;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.databaseTableSchema = databaseTableSchema;
        this.primaryKeyIndex = primaryKeyIndex;
        this.rowType = tableSchema.toPhysicalRowDataType();
    }

    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        ds.setDataSourceProperties(
                MapUtils.toProperties(jdbcSinkConfig.getJdbcConnectionConfig().getProperties()));
        if (jdbcSinkConfig
                .getJdbcConnectionConfig()
                .getDriverName()
                .equals("io.transwarp.jdbc.InceptorDriver")) {
            ds.setDriverClassName("io.transwarp.jdbc.InceptorDriver");
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUrl().startsWith("jdbc:phoenix:thin:")) {
            ds.setConnectionTestQuery("select 1");
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
            int queueIndex) {
        this.connectionProvider =
                new SimpleJdbcConnectionPoolProviderProxy(
                        multiTableResourceManager.getSharedResource().get(),
                        jdbcSinkConfig.getJdbcConnectionConfig(),
                        queueIndex);
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        tryOpen();
        try {
            outputFormat.flush();
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (Exception e) {
            log.error("Close jdbc sink writer failed.", e);
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "unable to close JDBC sink write",
                    e);
        } finally {
            outputFormat.close();
        }
    }

    @SneakyThrows
    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        log.info("received schema change event: " + event);
        try {
            outputFormat.close();
            final DataTypeChangeEventDispatcher dataTypeChangeEventDispatcher =
                    new DataTypeChangeEventDispatcher();
            dataTypeChangeEventDispatcher.reset(rowType);
            rowType = dataTypeChangeEventDispatcher.handle(event);
            CatalogTable preCatalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("default", "default", "default"),
                            tableSchema,
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            null);
            CatalogTable catalogTable = CatalogTableUtil.newCatalogTable(preCatalogTable, rowType);
            outputFormat =
                    new JdbcOutputFormatBuilder(
                                    dialect,
                                    connectionProvider,
                                    jdbcSinkConfig,
                                    catalogTable.getTableSchema(),
                                    databaseTableSchema)
                            .build();
            // Before OutputFormat opens, you need to update the database first
            JdbcConnectionProvider refreshTableSchemaConnectionProvider =
                    dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
            try (Connection connection =
                    refreshTableSchemaConnectionProvider.getOrEstablishConnection()) {
                dialect.applySchemaChange(connection, sinkTablePath, event);
            } catch (Exception throwables) {
                log.error("schema change error :", throwables);
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        "schema change error",
                        throwables);
            }
            outputFormat.open();
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    " rebuild outputFormat fail",
                    e);
        }
    }
}
