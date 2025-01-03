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

package org.apache.seatunnel.connectors.dolphindb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.dolphindb.catalog.DolphinDBCatalog;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;
import org.apache.seatunnel.connectors.dolphindb.utils.DolphinDBSaveModeUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DolphinDBSaveModeHandler extends DefaultSaveModeHandler {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final DolphinDBCatalog catalog;

    public DolphinDBSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            DolphinDBCatalog catalog,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig) {
        super(
                schemaSaveMode,
                dataSaveMode,
                catalog,
                catalogTable,
                readonlyConfig.get(DolphinDBConfig.CUSTOM_SQL));
        this.readonlyConfig = readonlyConfig;
        this.catalog = catalog;
        this.catalogTable = catalogTable;
    }

    @Override
    protected boolean tableExists() {
        return catalog.tableExists(catalogTable.getTableId().toTablePath());
    }

    @Override
    protected void dropTable() {
        catalog.dropTable(catalogTable.getTableId().toTablePath(), true);
    }

    @Override
    protected void createTable() {
        // todo: it's not a good idea to auto create database and table in dolphindb, since it's
        // rely on many params
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
        if (!catalog.databaseExists(database)) {
            catalog.createDatabase(TablePath.of(database, ""), true);
        }
        if (!catalog.tableExists(tablePath)) {
            String finalTemplate =
                    DolphinDBSaveModeUtil.fillingCreateSql(
                            readonlyConfig.get(DolphinDBConfig.SAVE_MODE_CREATE_TEMPLATE),
                            database,
                            tableName,
                            catalogTable.getTableSchema());
            log.info("Create table with sql: {}", finalTemplate);
            catalog.executeScript(finalTemplate);
        }
    }

    @Override
    protected void truncateTable() {
        catalog.truncateTable(catalogTable.getTableId().toTablePath(), true);
    }

    @Override
    protected boolean dataExists() {
        return catalog.isExistsData(catalogTable.getTableId().toTablePath());
    }

    @Override
    protected void executeCustomSql() {
        log.info("Executing custom SQL for table {} with SQL: {}", tablePath, customSql);
        catalog.executeScript(readonlyConfig.get(DolphinDBConfig.CUSTOM_SQL));
    }

    @Override
    public void close() throws Exception {
        try (DolphinDBCatalog closed = catalog) {}
    }
}
