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

package org.apache.seatunnel.connectors.seatunnel.sapbw.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.sapbw.client.SAPJcoClient;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBAPISinkConfig;

import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoRecordMetaData;
import com.sap.conn.jco.JCoTable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SAPBAPISinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final Map<String, CatalogTable> tables;
    private final Map<String, JCoTable> jCoTableMap;
    private final SAPBAPISinkConfig sapbapiSinkConfig;
    private final SAPJcoClient client;
    private final JCoFunction bapi;

    public SAPBAPISinkWriter(List<CatalogTable> tables, SAPBAPISinkConfig sapbapiSinkConfig)
            throws JCoException {
        this.tables =
                tables.stream()
                        .collect(
                                Collectors.toMap(
                                        table -> table.getTablePath().toString(),
                                        Function.identity()));
        this.sapbapiSinkConfig = sapbapiSinkConfig;
        this.client = SAPJcoClient.createClient(sapbapiSinkConfig);
        this.bapi =
                client.getDestination()
                        .getRepository()
                        .getFunction(sapbapiSinkConfig.getBapiName());
        this.jCoTableMap = new HashMap<>();
        for (CatalogTable table : tables) {
            String tablePath = table.getTablePath().toString();
            JCoTable jCoTable =
                    bapi.getTableParameterList().getTable(table.getTablePath().getTableName());
            if (jCoTable == null) {
                throw new RuntimeException(
                        "JCoTable not found in table parameter list for table: " + tablePath);
            }
            jCoTableMap.put(tablePath, jCoTable);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        JCoTable table = jCoTableMap.get(element.getTableId());
        if (table == null) {
            throw new IOException("JCoTable not found for table: " + element.getTableId());
        }
        table.appendRow();
        SeaTunnelRowType rowType = tables.get(element.getTableId()).getSeaTunnelRowType();
        for (int i = 0; i < rowType.getFieldTypes().length; i++) {
            table.setValue(
                    rowType.getFieldName(i),
                    getValue(rowType.getFieldTypes()[i], element.getField(i)));
        }
    }

    protected Object getValue(SeaTunnelDataType<?> dataType, Object val) {
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case STRING:
                return val;
            case DATE:
                return Date.from(
                        ((LocalDate) val).atStartOfDay(ZoneId.systemDefault()).toInstant());
            case TIME:
                return Date.from(
                        LocalDateTime.of(LocalDate.now(), (LocalTime) val)
                                .atZone(ZoneId.systemDefault())
                                .toInstant());
            case TIMESTAMP:
                return ((LocalDateTime) val).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(val);
            case BYTES:
                return new String((byte[]) val);
            default:
                throw new RuntimeException(
                        "Unsupported data type: " + dataType + " for SAP BAPI sink");
        }
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
        try {
            bapi.execute(client.getDestination());
            JCoTable returnTable =
                    bapi.getTableParameterList()
                            .getTable(sapbapiSinkConfig.getBapiReturnTableName());
            JCoRecordMetaData metaData = returnTable.getRecordMetaData();
            int fieldCount = metaData.getFieldCount();

            log.info("BAPI execution return:");
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                fieldNames.add(metaData.getName(i));
            }
            log.info(String.join("\t", fieldNames));
            log.info("--------------------------------------------------");
            for (int row = 0; row < returnTable.getNumRows(); row++) {
                returnTable.setRow(row);
                List<String> values = new ArrayList<>();
                for (int i = 0; i < fieldCount; i++) {
                    String fieldName = metaData.getName(i);
                    Object value = returnTable.getValue(fieldName);
                    values.add(String.valueOf(value));
                }
                log.info(String.join("\t", values));
            }
            log.info("--------------------------------------------------");
        } catch (JCoException e) {
            throw new IOException("Failed to execute BAPI: " + sapbapiSinkConfig.getBapiName(), e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
