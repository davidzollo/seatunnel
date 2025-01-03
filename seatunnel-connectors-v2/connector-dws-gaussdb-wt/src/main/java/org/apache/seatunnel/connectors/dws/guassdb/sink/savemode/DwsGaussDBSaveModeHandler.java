/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.dws.guassdb.sink.savemode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.CUSTOM_SQL;

@Slf4j
public class DwsGaussDBSaveModeHandler extends DefaultSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    private final CatalogTable catalogTable;
    private final DwsGaussSqlGenerator dwsGaussSqlGenerator;

    private final DwsGaussDBCatalog dwsGaussDBCatalog;

    public DwsGaussDBSaveModeHandler(
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            DwsGaussDBCatalog dwsGaussDBCatalog,
            DwsGaussSqlGenerator dwsGaussSqlGenerator) {
        super(
                readonlyConfig.get(DwsGaussDBSinkOption.SCHEMA_SAVE_MODE),
                readonlyConfig.get(DwsGaussDBSinkOption.DATA_SAVE_MODE),
                dwsGaussDBCatalog,
                catalogTable,
                readonlyConfig.get(DwsGaussDBSinkOption.CUSTOM_SQL));
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.dwsGaussSqlGenerator = dwsGaussSqlGenerator;
        this.dwsGaussDBCatalog = dwsGaussDBCatalog;
    }

    @Override
    protected boolean tableExists() {

        boolean targetTableExist =
                dwsGaussDBCatalog.tableExists(catalogTable.getTableId().toTablePath());
        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.APPEND_ONLY) {
            return targetTableExist;
        }
        boolean temporaryTableExist =
                dwsGaussDBCatalog.tableExists(
                        TablePath.of(
                                catalogTable.getTableId().getDatabaseName(),
                                catalogTable.getTableId().getSchemaName(),
                                dwsGaussSqlGenerator.getTemporaryTableName()));
        return targetTableExist && temporaryTableExist;
    }

    @Override
    protected void dropTable() {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTargetTableSql());
        log.info("Drop table: {} success", dwsGaussSqlGenerator.getTargetTableName());
        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTemporaryTableSql());
            log.info("Drop temporary table: {} success", dwsGaussSqlGenerator.getTargetTableName());
        }
    }

    @Override
    protected void createTable() {
        // We use IF NOT EXISTS to create table, so we don't need to check table exists
        dwsGaussDBCatalog.createTable(tablePath, catalogTable, false);
    }

    @Override
    protected void truncateTable() {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDeleteTargetTableSql());
        log.info("Delete data in table: {}", dwsGaussSqlGenerator.getTargetTableName());

        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDeleteTemporaryTableSql());
            log.info(
                    "Delete data in temporary table: {}",
                    dwsGaussSqlGenerator.getTemporaryTableName());
        }
    }

    @Override
    protected boolean dataExists() {
        return dwsGaussDBCatalog.queryDataCount(
                        dwsGaussSqlGenerator.getQuertTargetTableDataCountSql())
                > 0;
    }

    @Override
    protected void executeCustomSql() {
        log.info("Executing custom SQL for table {} with SQL: {}", tablePath, customSql);
        String customSql = readonlyConfig.get(CUSTOM_SQL);
        if (StringUtils.isEmpty(customSql)) {
            throw new IllegalArgumentException("The custom_sql is empty");
        }
        dwsGaussDBCatalog.executeSql(customSql);
        log.info("Execute custom sql success: {}", customSql);
    }

    @Override
    public void close() throws Exception {
        try (DwsGaussDBCatalog closed = dwsGaussDBCatalog) {}
    }
}
