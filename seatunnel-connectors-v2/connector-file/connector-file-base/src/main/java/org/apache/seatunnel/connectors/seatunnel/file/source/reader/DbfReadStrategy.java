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
package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.EncodingUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@NoArgsConstructor
public class DbfReadStrategy extends AbstractReadStrategy {

    private String encoding;

    @SneakyThrows
    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output) {
        Map<String, String> partitionsMap =
                isMergePartition ? parsePartitionsByPath(path) : Collections.emptyMap();
        try (FSDataInputStream file = openFile(path);
                DBFReader reader = new DBFReader(file, EncodingUtils.tryParseCharset(encoding))) {
            if (skipHeaderNumber > Integer.MAX_VALUE
                    || skipHeaderNumber < Integer.MIN_VALUE
                    || skipHeaderNumber > reader.getRecordCount()) {
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
            }
            reader.skipRecords((int) skipHeaderNumber);
            List<DBFField> dbfFields = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField field = reader.getField(i);
                dbfFields.add(field);
            }
            DbfRowDeserializer dbfRowTransformer =
                    new DbfRowDeserializer(dbfFields, seaTunnelRowType, partitionsMap);
            Object[] rowObjects;
            while ((rowObjects = reader.nextRecord()) != null) {
                // read current rowObjects
                SeaTunnelRow seaTunnelRow = dbfRowTransformer.deserilizeToSeatunnelRow(rowObjects);
                seaTunnelRow.setTableId(tableId);
                output.collect(seaTunnelRow);
            }
        }
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        if (ArrayUtils.isEmpty(seaTunnelRowType.getFieldNames())
                || ArrayUtils.isEmpty(seaTunnelRowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Schmea information is not set or incorrect schema settings");
        }
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        this.encoding =
                readonlyConfig
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            int[] indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = seaTunnelRowType.indexOf(readColumns.get(i));
                fields[i] = seaTunnelRowType.getFieldName(indexes[i]);
                types[i] = seaTunnelRowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = seaTunnelRowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }

    public static class DbfRowDeserializer {
        private final DBFDataType[] dbfDataTypes;

        private final Map<Integer, Integer> seaTunnelRowFieldIndexInDbfIndexMapping;

        private final SeaTunnelRowType seaTunnelRowType;
        private final Map<String, String> partitionsMap;

        private final int totalFieldCount;

        public DbfRowDeserializer(
                List<DBFField> dbfFields,
                SeaTunnelRowType seaTunnelRowType,
                Map<String, String> partitionsMap) {
            this.seaTunnelRowType = seaTunnelRowType;
            this.partitionsMap = partitionsMap;
            this.totalFieldCount = seaTunnelRowType.getTotalFields() + partitionsMap.size();

            log.info("The current dbf schema is \n{}", dbfFields);

            this.seaTunnelRowFieldIndexInDbfIndexMapping = new HashMap<>();
            for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                String fieldName = seaTunnelRowType.getFieldName(i);
                for (int j = 0; j < dbfFields.size(); j++) {
                    DBFField dbfField = dbfFields.get(j);
                    if (dbfField.getName().equals(fieldName)) {
                        seaTunnelRowFieldIndexInDbfIndexMapping.put(i, j);
                        break;
                    }
                }
                if (!seaTunnelRowFieldIndexInDbfIndexMapping.containsKey(i)) {
                    throw new IllegalArgumentException(
                            "can't find field [" + fieldName + "] in dbf file: " + dbfFields);
                }
            }

            this.dbfDataTypes = new DBFDataType[dbfFields.size()];
            for (int i = 0; i < dbfFields.size(); i++) {
                dbfDataTypes[i] = dbfFields.get(i).getType();
            }
        }

        public SeaTunnelRow deserilizeToSeatunnelRow(Object[] objects) {
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(totalFieldCount);

            for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                Integer dbfIndex = seaTunnelRowFieldIndexInDbfIndexMapping.get(i);
                Object dbfObject = objects[dbfIndex];
                DBFDataType dbfDataType = dbfDataTypes[dbfIndex];
                Object seatunnelObject =
                        convertDbfDataToSeaTunnelData(
                                dbfObject, dbfDataType, seaTunnelRowType.getFieldType(i));
                seaTunnelRow.setField(i, seatunnelObject);
            }
            if (seaTunnelRowType.getTotalFields() < totalFieldCount) {
                int i = seaTunnelRowType.getTotalFields();
                for (String value : partitionsMap.values()) {
                    seaTunnelRow.setField(i++, value);
                }
            }
            return seaTunnelRow;
        }

        private Object convertDbfDataToSeaTunnelData(
                Object dbfData, DBFDataType dbfDataType, SeaTunnelDataType seaTunnelDataType) {
            SqlType sqlType = seaTunnelDataType.getSqlType();
            switch (sqlType) {
                case NULL:
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case STRING: // CHARACTER
                case BYTES: // VARCHAR, VARBINARY, BINARY, PICTURE
                case DECIMAL: // CURRENCY
                    return dbfData;
                case DATE:
                    if (dbfData == null) {
                        return null;
                    }
                    Instant instant1 = Instant.ofEpochMilli(((Date) dbfData).getTime());
                    return LocalDateTime.ofInstant(instant1, ZoneId.systemDefault()).toLocalDate();
                case TIME:
                    if (dbfData == null) {
                        return null;
                    }
                    Instant instant2 = Instant.ofEpochMilli(((Date) dbfData).getTime());
                    return LocalDateTime.ofInstant(instant2, ZoneId.systemDefault()).toLocalTime();
                case TIMESTAMP:
                    if (dbfData == null) {
                        return null;
                    }
                    Instant instant3 = Instant.ofEpochMilli(((Date) dbfData).getTime());
                    return LocalDateTime.ofInstant(instant3, ZoneId.systemDefault());
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported type: "
                                    + seaTunnelDataType.getSqlType()
                                    + " the dbfType is "
                                    + dbfDataType
                                    + " the dbfData is "
                                    + dbfData);
            }
        }
    }
}
