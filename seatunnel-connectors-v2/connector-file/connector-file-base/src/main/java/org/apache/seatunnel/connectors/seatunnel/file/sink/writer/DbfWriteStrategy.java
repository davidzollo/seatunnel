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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFFileFormat;
import com.linuxense.javadbf.DBFWriter;
import lombok.NonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;

public class DbfWriteStrategy extends AbstractWriteStrategy<DBFWriter> {
    private final LinkedHashMap<String, DBFWriter> beingWrittenWriter;

    private DbfSerializer dbfSerializer;

    public DbfWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        this.dbfSerializer =
                new DbfSerializer(
                        buildSchemaWithRowType(seaTunnelRowType, sinkColumnsIndexInRow),
                        fileSinkConfig);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        DBFWriter dbfWriter = getOrCreateOutputStream(filePath);
        Object[] dbfRow = dbfSerializer.serializeToDbfRow(seaTunnelRow);
        dbfWriter.addRecord(dbfRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    v.close();
                    needMoveFiles.put(k, getTargetLocation(k));
                });
    }

    @Override
    public DBFWriter getOrCreateOutputStream(@NonNull String filePath) {
        DBFWriter dbfWriter = beingWrittenWriter.get(filePath);
        if (dbfWriter != null) {
            return dbfWriter;
        }
        try {

            FSDataOutputStream outputStream = hadoopFileSystemProxy.getOutputStream(filePath);
            DBFWriter newWriter = null;
            switch (fileSinkConfig.getDbfVersion()) {
                case DEFAULT:
                    newWriter =
                            new DBFWriter(
                                    outputStream, StandardCharsets.UTF_8, DBFFileFormat.COMPATIBLE);
                    break;
                case DB7:
                    newWriter =
                            new DBFWriter(
                                    outputStream, StandardCharsets.UTF_8, DBFFileFormat.ADVANCED);
                    break;
            }
            newWriter.setFields(dbfSerializer.getDbfFields());
            beingWrittenWriter.put(filePath, newWriter);
            return newWriter;
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED, "can not get output file stream");
        }
    }

    private static class DbfSerializer {
        private final DBFField[] dbfFields;
        private final SeaTunnelRowType seaTunnelRowType;

        public DbfSerializer(SeaTunnelRowType seaTunnelRowType, FileSinkConfig fileSinkConfig) {
            this.seaTunnelRowType = seaTunnelRowType;
            this.dbfFields = new DBFField[seaTunnelRowType.getTotalFields()];

            for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                String fieldName = seaTunnelRowType.getFieldName(i);
                dbfFields[i] = new DBFField();
                dbfFields[i].setName(fieldName);
                dbfFields[i].setType(convertToDbfType(seaTunnelRowType.getFieldType(i)));
                // TODO: Configure according to user configuration
                switch (dbfFields[i].getType()) {
                    case CHARACTER:
                        switch (fileSinkConfig.getDbfVersion()) {
                            case DEFAULT:
                                dbfFields[i].setLength(255);
                                break;
                            case DB7:
                                dbfFields[i].setLength(DBFDataType.CHARACTER.getMaxSize());
                                break;
                        }
                        break;
                    case NUMERIC:
                        dbfFields[i].setLength(DBFDataType.NUMERIC.getMaxSize());
                        break;
                    case CURRENCY:
                        dbfFields[i].setLength(DBFDataType.CURRENCY.getMaxSize());
                        break;
                    case DATE:
                        dbfFields[i].setLength(DBFDataType.DATE.getMaxSize());
                        break;
                    case VARCHAR:
                        dbfFields[i].setLength(DBFDataType.VARCHAR.getMaxSize());
                        break;
                    case LOGICAL:
                        dbfFields[i].setLength(DBFDataType.LOGICAL.getMaxSize());
                    default:
                }
            }
        }

        public DBFField[] getDbfFields() {
            return dbfFields;
        }

        public Object[] serializeToDbfRow(SeaTunnelRow seaTunnelRow) {
            SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();

            Object[] fields = seaTunnelRow.getFields();
            Object[] dbfRow = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                dbfRow[i] =
                        convertSeaTunnelObjectToDbfObject(fields[i], dbfFields[i], fieldTypes[i]);
            }
            return dbfRow;
        }

        private DBFDataType convertToDbfType(SeaTunnelDataType seaTunnelDataType) {
            SqlType sqlType = seaTunnelDataType.getSqlType();
            switch (sqlType) {
                case STRING:
                    return DBFDataType.CHARACTER;
                case BOOLEAN:
                    return DBFDataType.LOGICAL;
                case TINYINT:
                case SMALLINT:
                case DOUBLE:
                case INT:
                case BIGINT:
                case FLOAT:
                    return DBFDataType.NUMERIC;
                case DECIMAL:
                    return DBFDataType.CURRENCY;
                case BYTES:
                    return DBFDataType.VARCHAR;
                case DATE:
                case TIME:
                case TIMESTAMP:
                    return DBFDataType.DATE;
                default:
                    throw new UnsupportedOperationException(
                            sqlType + " type is not supported in DBF");
            }
        }

        private Object convertSeaTunnelObjectToDbfObject(
                Object seatunnelObject, DBFField dbfField, SeaTunnelDataType seaTunnelDataType) {
            SqlType sqlType = seaTunnelDataType.getSqlType();

            switch (sqlType) {
                case STRING:
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case BYTES:
                    return seatunnelObject;
                case TIMESTAMP:
                    if (seatunnelObject == null) {
                        return null;
                    }
                    return new Date(
                            ((LocalDateTime) seatunnelObject)
                                    .toInstant(ZoneOffset.UTC)
                                    .toEpochMilli());
                default:
                    throw new UnsupportedOperationException(
                            sqlType + " type is not supported in DBF");
            }
        }
    }
}
