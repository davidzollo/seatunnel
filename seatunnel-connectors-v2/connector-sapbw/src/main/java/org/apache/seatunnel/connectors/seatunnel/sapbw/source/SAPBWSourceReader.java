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

package org.apache.seatunnel.connectors.seatunnel.sapbw.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.sapbw.catalog.SAPBWDataConverter;
import org.apache.seatunnel.connectors.seatunnel.sapbw.client.SAPJcoClient;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBWSourceConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
public class SAPBWSourceReader implements SourceReader<SeaTunnelRow, SAPBWSplit> {

    private final SourceReader.Context context;
    private final Deque<SAPBWSplit> splits = new ConcurrentLinkedDeque<>();
    private final SAPBWSourceConfig sourceConfig;
    private final Map<TablePath, CatalogTable> catalogTables;
    private final SAPBWDataConverter dataConverter = new SAPBWDataConverter();
    private SAPJcoClient client;
    private volatile boolean noMoreSplit;

    public SAPBWSourceReader(
            SourceReader.Context context,
            SAPBWSourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.catalogTables = catalogTables;
    }

    @Override
    public void open() {
        client = SAPJcoClient.createClient(sourceConfig);
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close SAP BW client", e);
            } finally {
                client = null;
            }
        }
    }

    @Override
    @SuppressWarnings("MagicNumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        synchronized (output.getCheckpointLock()) {
            SAPBWSplit split = splits.poll();
            if (null != split) {
                try {
                    queryRowFromSplit(split, catalogTables.get(split.getTablePath()), output);
                } catch (JCoException e) {
                    log.error("Error querying data from SAP BW: {}", e.getMessage());
                    throw new RuntimeException("Error querying data from SAP BW", e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("wait split!");
                }
            }
        }
        if (noMoreSplit
                && splits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded SAP BW source [{}]", context.getIndexOfSubtask());
            context.signalNoMoreElement();
        }
        Thread.sleep(1000L);
    }

    private void queryRowFromSplit(
            SAPBWSplit split, CatalogTable catalogTable, Collector<SeaTunnelRow> output)
            throws JCoException {
        JCoFunction getHierarchys =
                client.getDestination()
                        .getRepository()
                        .getFunction("BAPI_MDPROVIDER_GET_HIERARCHYS");
        getHierarchys
                .getImportParameterList()
                .setValue("CAT_NAM", catalogTable.getTablePath().getDatabaseName());
        getHierarchys
                .getImportParameterList()
                .setValue("CUBE_NAM", catalogTable.getTablePath().getTableName());
        getHierarchys.execute(client.getDestination());

        JCoTable hierarchies = getHierarchys.getTableParameterList().getTable("HIERARCHIES");
        List<String> hierarchyNames = new ArrayList<>();
        for (int i = 0; i < hierarchies.getNumRows(); i++) {
            hierarchies.setRow(i);
            String hierarchyName = hierarchies.getString("HRY_UNAM");
            if (!hierarchyName.equalsIgnoreCase("[Measures]")) {
                hierarchyNames.add(hierarchyName);
            }
        }
        List<String> mdxQueryLines = new ArrayList<>();
        mdxQueryLines.add("SELECT [Measures].Members ON COLUMNS, NON EMPTY {");
        mdxQueryLines.addAll(buildStarJoinWithIsLeaf(hierarchyNames, split.getMemberSplit()));
        mdxQueryLines.add("} ON ROWS FROM [" + catalogTable.getTablePath().getTableName() + "]");

        String variables =
                sourceConfig.getQueryTableConfigs().get(catalogTable.getTablePath()).getVariables();
        if (StringUtils.isNotEmpty(variables)) {
            mdxQueryLines.add("SAP VARIABLES");
            String[] variableLines = variables.split("\n");
            mdxQueryLines.addAll(Arrays.asList(variableLines));
        }

        String mdxQuery = String.join("\n", mdxQueryLines);
        log.info("MDX Query: {}", mdxQuery);

        String datasetid = null;
        try {
            JCoContext.begin(client.getDestination());
            JCoFunction function =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_CREATE_OBJECT");
            JCoTable commandTextTable = function.getTableParameterList().getTable("COMMAND_TEXT");
            for (String line : mdxQueryLines) {
                commandTextTable.appendRow();
                commandTextTable.setValue("LINE", line);
            }

            function.execute(client.getDestination());
            datasetid = function.getExportParameterList().getString("DATASETID");

            JCoFunction checkSyntax =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_CHECK_SYNTAX");
            checkSyntax.getImportParameterList().setValue("DATASETID", datasetid);
            checkSyntax.execute(client.getDestination());

            JCoFunction selectData =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_SELECT_DATA");
            selectData.getImportParameterList().setValue("DATASETID", datasetid);
            selectData.execute(client.getDestination());
            JCoStructure selectResult = selectData.getExportParameterList().getStructure("RETURN");
            if (selectResult.getString("TYPE").equalsIgnoreCase("E")) {
                String id = selectResult.getString("ID");
                String number = selectResult.getString("NUMBER");
                String errorMessage = selectResult.getString("MESSAGE");
                log.error("Error in MDX query: {}", errorMessage);
                throw new RuntimeException(
                        String.format(
                                "MDX query %s failed: %s-%s, %s",
                                mdxQuery, id, number, errorMessage));
            }

            JCoFunction axisFunc =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_GET_AXIS_DATA");
            // 0 on columns 1 on rows
            axisFunc.getImportParameterList().setValue("AXIS", "0");
            axisFunc.getImportParameterList().setValue("DATASETID", datasetid);
            axisFunc.execute(client.getDestination());
            JCoTable axisTable = axisFunc.getTableParameterList().getTable("MNDTRY_PRPTYS");
            List<String> axisList = new ArrayList<>();
            for (int i = 0; i < axisTable.getNumRows(); i++) {
                axisTable.setRow(i);
                axisList.add(axisTable.getString("MEM_UNAM"));
            }

            JCoFunction axis1Func =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_GET_AXIS_DATA");
            // 0 on columns 1 on rows
            axis1Func.getImportParameterList().setValue("AXIS", "1");
            axis1Func.getImportParameterList().setValue("DATASETID", datasetid);
            axis1Func.execute(client.getDestination());
            JCoTable axis1Table = axis1Func.getTableParameterList().getTable("MNDTRY_PRPTYS");
            Map<Integer, Map<Integer, Pair<String, Object>>> axis1Map = new HashMap<>();
            for (int i = 0; i < axis1Table.getNumRows(); i++) {
                axis1Table.setRow(i);
                int tuple = axis1Table.getInt("TUPLE_ORDINAL");
                int di = axis1Table.getInt("DIM_KEY");
                String measureName = axis1Table.getString("MEM_UNAM");
                axis1Map.computeIfAbsent(tuple, k -> new HashMap<>())
                        .put(
                                di,
                                Pair.of(
                                        getFieldNameByMeasureName(measureName),
                                        measureName.endsWith(".[#]")
                                                ? null
                                                : axis1Table.getValue("MEM_CAP")));
            }
            List<Object[]> values = new ArrayList<>();
            axis1Map.entrySet().stream()
                    .sorted((Map.Entry.comparingByKey()))
                    .forEach(
                            entry -> {
                                Map<Integer, Pair<String, Object>> valueMap = entry.getValue();
                                Object[] valueArray =
                                        new Object
                                                [catalogTable
                                                        .getSeaTunnelRowType()
                                                        .getTotalFields()];
                                valueMap.entrySet().stream()
                                        .sorted(Map.Entry.comparingByKey())
                                        .forEach(
                                                valueEntry -> {
                                                    Pair<String, Object> value =
                                                            valueEntry.getValue();
                                                    String measureName = value.getLeft();
                                                    Object measureValue = value.getRight();
                                                    int index =
                                                            catalogTable
                                                                    .getSeaTunnelRowType()
                                                                    .indexOf(measureName);
                                                    if (index == -1) {
                                                        throw new RuntimeException(
                                                                "Measure "
                                                                        + measureName
                                                                        + " not found in schema");
                                                    }
                                                    valueArray[index] = measureValue;
                                                });
                                values.add(valueArray);
                            });

            JCoFunction cellFunc =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDDATASET_GET_CELL_DATA");
            cellFunc.getImportParameterList().setValue("DATASETID", datasetid);
            cellFunc.execute(client.getDestination());
            JCoTable cellTable = cellFunc.getTableParameterList().getTable("CELL_DATA");
            Map<Integer, String> cellMap = new LinkedHashMap<>();
            for (int i = 0; i < cellTable.getNumRows(); i++) {
                cellTable.setRow(i);
                String status = cellTable.getString("CELL_STATUS");
                if (!status.equalsIgnoreCase("N")) {
                    cellMap.put(cellTable.getInt("CELL_ORDINAL"), cellTable.getString("VALUE"));
                }
            }

            List<String> valuesInCell =
                    cellMap.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
            for (int i = 0; i < valuesInCell.size(); i++) {
                String measureName = axisList.get(i % axisList.size());
                int index = catalogTable.getSeaTunnelRowType().indexOf(measureName);
                if (index == -1) {
                    throw new RuntimeException("Measure " + measureName + " not found in schema");
                }
                values.get(i / axisList.size())[index] = valuesInCell.get(i);
            }

            for (Object[] value : values) {
                SeaTunnelDataType<?>[] types = catalogTable.getSeaTunnelRowType().getFieldTypes();
                for (int i = 0; i < value.length; i++) {
                    if (value[i] == null) {
                        value[i] = null;
                    } else if (value[i] instanceof String && "#".equals(value[i])) {
                        value[i] = null;
                    } else {
                        value[i] = dataConverter.convert(types[i], value[i]);
                    }
                }
                SeaTunnelRow row = new SeaTunnelRow(value);
                output.collect(row);
            }
        } finally {
            if (StringUtils.isNotEmpty(datasetid)) {
                JCoFunction deleteObject =
                        client.getDestination()
                                .getRepository()
                                .getFunction("BAPI_MDDATASET_DELETE_OBJECT");
                deleteObject.getImportParameterList().setValue("DATASETID", datasetid);
                deleteObject.execute(client.getDestination());
            }
            JCoContext.end(client.getDestination());
        }
    }

    private static List<String> buildStarJoinWithIsLeaf(
            List<String> hierarchyNames, List<String> memberSplit) {
        if (hierarchyNames == null || hierarchyNames.isEmpty()) {
            throw new IllegalArgumentException("Hierarchy names cannot be null or empty");
        }
        return Arrays.asList(
                hierarchyNames.stream()
                        .map(
                                dimension -> {
                                    if (!memberSplit.isEmpty()
                                            && memberSplit.get(0).startsWith(dimension)) {
                                        return "{ " + String.join(" ,\n", memberSplit) + " }";
                                    }
                                    return String.format(
                                            "FILTER(%s.Members, IsLeaf(%s.CurrentMember))",
                                            dimension, dimension);
                                })
                        .collect(Collectors.joining(" *\n"))
                        .split("\n"));
    }

    private String getFieldNameByMeasureName(String measure) {
        return measure.substring(1, measure.indexOf(']'));
    }

    @Override
    public List<SAPBWSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<SAPBWSplit> splits) {
        log.debug("reader {} add splits {}", context.getIndexOfSubtask(), splits);
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
