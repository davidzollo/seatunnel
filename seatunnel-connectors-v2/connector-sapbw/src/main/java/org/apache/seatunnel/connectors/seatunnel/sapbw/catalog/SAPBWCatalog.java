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

package org.apache.seatunnel.connectors.seatunnel.sapbw.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.sapbw.client.SAPJcoClient;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBWSourceConfig;

import org.apache.commons.lang3.StringUtils;

import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SAPBWCatalog implements Catalog {

    private final SAPBWSourceConfig config;
    private final String catalogName;
    private SAPJcoClient client;

    public SAPBWCatalog(String catalogName, ReadonlyConfig options) {
        this.catalogName = catalogName;
        this.config = new SAPBWSourceConfig(options);
    }

    @Override
    public void open() throws CatalogException {
        client = SAPJcoClient.createClient(config);
    }

    @Override
    public void close() throws CatalogException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new CatalogException("Failed to close SAP BW client", e);
            } finally {
                client = null;
            }
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "$INFOCUBE";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            JCoFunction getCatalog =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_CATALOGS");
            getCatalog.execute(client.getDestination());
            JCoTable catalogs = getCatalog.getTableParameterList().getTable("CATALOGS");
            List<String> catalogList = new ArrayList<>();
            for (int i = 0; i < catalogs.getNumRows(); i++) {
                catalogs.setRow(i);
                catalogList.add(catalogs.getString("CAT_NAM"));
            }
            return catalogList;
        } catch (JCoException e) {
            throw new CatalogException("Failed to list databases from SAP BW", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            JCoFunction getCubes =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_CUBES");
            getCubes.getImportParameterList().setValue("CAT_NAM", databaseName);
            getCubes.execute(client.getDestination());
            JCoTable cubes = getCubes.getTableParameterList().getTable("CUBES");
            List<String> cubeList = new ArrayList<>();
            for (int i = 0; i < cubes.getNumRows(); i++) {
                cubes.setRow(i);
                cubeList.add(cubes.getString("CUBE_NAM"));
            }
            return cubeList;
        } catch (JCoException e) {
            throw new CatalogException("Failed to list tables from SAP BW", e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return getTable(tablePath, Collections.emptyList());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath, List<String> fieldNames)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        try {
            JCoFunction getHierarchys =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_HIERARCHYS");
            getHierarchys.getImportParameterList().setValue("CAT_NAM", tablePath.getDatabaseName());
            getHierarchys.getImportParameterList().setValue("CUBE_NAM", tablePath.getTableName());
            getHierarchys.execute(client.getDestination());

            JCoTable hierarchies = getHierarchys.getTableParameterList().getTable("HIERARCHIES");
            List<String> hierarchyNames = new ArrayList<>();
            for (int i = 0; i < hierarchies.getNumRows(); i++) {
                hierarchies.setRow(i);
                String hierarchyName = hierarchies.getString("HRY_UNAM");
                if (!hierarchyName.equalsIgnoreCase("[Measures]")) {
                    hierarchyNames.add(hierarchyName.substring(1, hierarchyName.length() - 1));
                }
            }

            buildColumnsWithErrorCheck(
                    tablePath,
                    builder,
                    hierarchyNames.stream()
                            .filter(
                                    n ->
                                            fieldNames == null
                                                    || fieldNames.isEmpty()
                                                    || fieldNames.contains(n))
                            .iterator(),
                    this::getColumnFromInfoObject);

            JCoFunction getMeasures =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_MEASURES");
            getMeasures.getImportParameterList().setValue("CAT_NAM", tablePath.getDatabaseName());
            getMeasures.getImportParameterList().setValue("CUBE_NAM", tablePath.getTableName());
            getMeasures.execute(client.getDestination());
            JCoTable measures = getMeasures.getTableParameterList().getTable("MEASURES");
            for (int i = 0; i < measures.getNumRows(); i++) {
                measures.setRow(i);
                String measureName = measures.getString("MES_UNAM");
                String description = measures.getString("DSCRPTN");
                String dataType = measures.getString("DATA_TYPE");
                Long length = Long.parseLong(measures.getString("NUM_PREC"));
                Integer scale = Integer.parseInt(measures.getString("NUM_SCALE"));
                builder.column(
                        getColumnFromMeasure(measureName, description, dataType, length, scale));
            }

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
            JCoFunction getCubes =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_CUBES");
            getCubes.getImportParameterList().setValue("CAT_NAM", tablePath.getDatabaseName());
            getCubes.getImportParameterList().setValue("CUBE_NAM", tablePath.getTableName());
            getCubes.execute(client.getDestination());
            JCoTable cubes = getCubes.getTableParameterList().getTable("CUBES");
            cubes.setRow(0);
            String tableName = cubes.getString("DSCRPTN");

            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    tableName);
        } catch (JCoException e) {
            throw new CatalogException("Failed to get table from SAP BW", e);
        }
    }

    private Column getColumnFromMeasure(
            String measureName, String description, String dataType, Long length, Integer scale) {
        BasicTypeDefine<String> define =
                BasicTypeDefine.<String>builder()
                        .name(measureName)
                        .dataType(dataType)
                        .columnType(dataType)
                        .length(length)
                        .precision(length)
                        .scale(scale)
                        .comment(description)
                        .build();
        return SAPBWTypeConverter.INSTANCE.convert(define);
    }

    private Column getColumnFromInfoObject(String infoObject) {
        try {
            JCoFunction getFieldType =
                    client.getDestination().getRepository().getFunction("BAPI_IOBJ_GETDETAIL");
            getFieldType.getImportParameterList().setValue("INFOOBJECT", infoObject);
            getFieldType.execute(client.getDestination());
            JCoStructure details = getFieldType.getExportParameterList().getStructure("DETAILS");
            String dataType = details.getString("DATATP");
            Long length = Long.parseLong(details.getString("LENG"));
            String decimalPlaces = details.getString("DECIMALS");
            boolean nullable = details.getString("NOVALFL").equalsIgnoreCase("X");
            String comment = details.getString("TEXTLONG");
            if (StringUtils.isEmpty(comment)) {
                comment = details.getString("TEXTSHORT");
            }

            BasicTypeDefine<String> define =
                    BasicTypeDefine.<String>builder()
                            .name(infoObject)
                            .dataType(dataType)
                            .columnType(dataType)
                            .length(length)
                            .precision(length)
                            .scale(Integer.parseInt(decimalPlaces))
                            .nullable(nullable)
                            .comment(comment)
                            .build();
            return SAPBWTypeConverter.INSTANCE.convert(define);

        } catch (JCoException e) {
            throw new CatalogException("Failed to get column from SAP BW", e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Create table is not supported in SAP BW catalog");
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Drop table is not supported in SAP BW catalog");
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Create database is not supported in SAP BW catalog");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Drop database is not supported in SAP BW catalog");
    }
}
