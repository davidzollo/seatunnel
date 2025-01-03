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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3RedshiftSaveModeHandler extends DefaultSaveModeHandler {
    private final S3RedshiftSQLGenerator sqlGenerator;
    private final S3RedshiftConf conf;
    private final RedshiftJdbcClient redshiftJdbcClient;

    public S3RedshiftSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql,
            S3RedshiftSQLGenerator sqlGenerator,
            S3RedshiftConf conf) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.sqlGenerator = sqlGenerator;
        this.conf = conf;
        this.redshiftJdbcClient = RedshiftJdbcClient.newSingleConnection(conf);
    }

    @Override
    protected void recreateSchema() {
        if (conf.notAppendOnlyMode()) {
            dropTemporaryTable();
            createTemporaryTable();
        }
        dropTable();
        createTable();
    }

    @SneakyThrows
    @Override
    protected void createSchemaWhenNotExist() {
        if (conf.notAppendOnlyMode()) {
            if (!temporaryTableExists()) {
                createTemporaryTable();
            }
        }

        if (!tableExists()) {
            createTable();
        }
    }

    @Override
    protected void errorWhenSchemaNotExist() {
        if (conf.notAppendOnlyMode()) {
            if (!temporaryTableExists()) {
                createTemporaryTable();
            }
        }
        super.errorWhenSchemaNotExist();
    }

    @Override
    protected void keepSchemaDropData() {
        if (conf.notAppendOnlyMode()) {
            if (temporaryTableExists()) {
                truncateTemporaryTable();
            }
        }
        if (tableExists()) {
            truncateTable();
        }
    }

    @Override
    protected void errorWhenDataExists() {
        if (conf.notAppendOnlyMode()) {
            if (temporaryDataExists()) {
                truncateTemporaryTable();
            }
        }
        super.errorWhenDataExists();
    }

    @SneakyThrows
    @Override
    public boolean tableExists() {
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistTableSql());
    }

    @SneakyThrows
    public boolean temporaryTableExists() {
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistTemporaryTableSql());
    }

    @SneakyThrows
    @Override
    public void dropTable() {
        log.info("Executing Drop Table with SQL: {}", sqlGenerator.getDropTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getDropTableSql());
    }

    @SneakyThrows
    public void dropTemporaryTable() {
        log.info(
                "Executing Drop Temporary Table with SQL: {}",
                sqlGenerator.getDropTemporaryTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getDropTemporaryTableSql());
    }

    @SneakyThrows
    @Override
    public void createTable() {
        log.info("Executing Create Table with SQL: {}", sqlGenerator.getCreateTableSQL());
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
    }

    @SneakyThrows
    public void createTemporaryTable() {
        log.info(
                "Executing Create Temporary Table with SQL: {}",
                sqlGenerator.getCreateTemporaryTableSQL());
        redshiftJdbcClient.execute(sqlGenerator.getCreateTemporaryTableSQL());
    }

    @SneakyThrows
    @Override
    public void truncateTable() {
        log.info("Executing Truncate Table with SQL: {}", sqlGenerator.getCleanTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getCleanTableSql());
    }

    @SneakyThrows
    public void truncateTemporaryTable() {
        log.info(
                "Executing Truncate Temporary Table with SQL: {}",
                sqlGenerator.getCleanTemporaryTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getCleanTemporaryTableSql());
    }

    @SneakyThrows
    @Override
    public boolean dataExists() {
        log.info("Executing Data Exists with SQL: {}", sqlGenerator.getIsExistDataSql());
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistDataSql());
    }

    @SneakyThrows
    public boolean temporaryDataExists() {
        log.info(
                "Executing Temporary Data Exists with SQL: {}",
                sqlGenerator.getIsExistTemporaryDataSql());
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistTemporaryDataSql());
    }

    @SneakyThrows
    @Override
    public void executeCustomSql() {
        log.info("Executing custom SQL for table {} with SQL: {}", tablePath, customSql);
        redshiftJdbcClient.execute(conf.getCustomSql());
    }

    @Override
    public void close() {
        redshiftJdbcClient.close();
    }
}
