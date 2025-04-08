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
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashan;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference
// https://doc.yashandb.com/yashandb/23.2/zh/Development-Guide/SQL-Reference-Manual/Data-Types/00Data-Types.html
@Slf4j
@AutoService(TypeConverter.class)
public class YashanTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String YASHAN_NUMBER = "NUMBER";
    public static final String YASHAN_INTEGER = "INTEGER";
    public static final String YASHAN_FLOAT = "FLOAT";
    public static final String YASHAN_DOUBLE = "DOUBLE";
    public static final String YASHAN_DOUBLE_PRECISION = "DOUBLE PRECISION";

    public static final String YASHAN_CHAR = "CHAR";
    public static final String YASHAN_VARCHAR = "VARCHAR";
    public static final String YASHAN_VARCHAR2 = "VARCHAR2";
    public static final String YASHAN_NVARCHAR2 = "NVARCHAR2";
    public static final String YASHAN_CLOB = "CLOB";
    public static final String YASHAN_NCLOB = "NCLOB";

    public static final String YASHAN_DATE = "DATE";
    public static final String YASHAN_TIMESTAMP = "TIMESTAMP";
    public static final String YASHAN_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";

    public static final String YASHAN_BLOB = "BLOB";
    public static final String YASHAN_RAW = "RAW";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = 127;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_TIMESTAMP_SCALE = 9;
    public static final int MAX_RAW_LENGTH = 2000;
    public static final int MAX_VARCHAR2_LENGTH = 4000;

    public static final YashanTypeConverter INSTANCE = new YashanTypeConverter();

    private final boolean decimalNarrowingEnabled;

    public YashanTypeConverter() {
        this(true);
    }

    public YashanTypeConverter(boolean decimalNarrowingEnabled) {
        this.decimalNarrowingEnabled = decimalNarrowingEnabled;
    }

    @Override
    public String identifier() {
        return DatabaseIdentifier.YASHAN;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String yashanType = typeDefine.getDataType().toUpperCase();
        switch (yashanType) {
            case YASHAN_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case YASHAN_NUMBER:
                handleNumberType(builder, typeDefine);
                break;
            case YASHAN_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case YASHAN_DOUBLE:
            case YASHAN_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case YASHAN_CHAR:
            case YASHAN_VARCHAR:
            case YASHAN_VARCHAR2:
            case YASHAN_NVARCHAR2:
                handleStringType(builder, typeDefine);
                break;
            case YASHAN_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case YASHAN_TIMESTAMP:
            case YASHAN_TIMESTAMP_WITH_TIME_ZONE:
                handleTimestampType(builder, typeDefine);
                break;
            case YASHAN_BLOB:
            case YASHAN_RAW:
                handleBinaryType(builder, typeDefine);
                break;
            case YASHAN_CLOB:
            case YASHAN_NCLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(Long.MAX_VALUE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.YASHAN, yashanType, typeDefine.getName());
        }
        return builder.build();
    }

    private void handleNumberType(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        Long precision =
                typeDefine.getPrecision() != null ? typeDefine.getPrecision() : DEFAULT_PRECISION;
        Integer scale = typeDefine.getScale() != null ? typeDefine.getScale() : DEFAULT_SCALE;

        if (scale <= 0) {
            if (decimalNarrowingEnabled) {
                if (precision <= 9) {
                    builder.dataType(BasicType.INT_TYPE);
                } else if (precision <= 18) {
                    builder.dataType(BasicType.LONG_TYPE);
                } else {
                    builder.dataType(new DecimalType(precision.intValue(), 0));
                }
            } else {
                builder.dataType(new DecimalType(precision.intValue(), 0));
            }
        } else {
            builder.dataType(new DecimalType(precision.intValue(), scale));
        }

        builder.columnLength(precision);
        builder.scale(scale);
    }

    private void handleStringType(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        builder.dataType(BasicType.STRING_TYPE);
        Long length = typeDefine.getLength();
        if (length == null || length <= 0) {
            length = (long) MAX_VARCHAR2_LENGTH;
        } else if (length > MAX_VARCHAR2_LENGTH) {
            log.warn(
                    "VARCHAR2 length {} exceeds maximum {}, truncating to {}",
                    length,
                    MAX_VARCHAR2_LENGTH,
                    MAX_VARCHAR2_LENGTH);
            length = (long) MAX_VARCHAR2_LENGTH;
        }
        builder.columnLength(length);
    }

    private void handleTimestampType(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        Integer scale = typeDefine.getScale();
        if (scale == null || scale < 0 || scale > MAX_TIMESTAMP_SCALE) {
            scale = MAX_TIMESTAMP_SCALE;
            log.warn(
                    "TIMESTAMP scale {} is invalid, using default {}",
                    typeDefine.getScale(),
                    scale);
        }
        builder.scale(scale);
    }

    private void handleBinaryType(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        builder.dataType(PrimitiveByteArrayType.INSTANCE);
        Long length = typeDefine.getLength();
        if (YASHAN_RAW.equalsIgnoreCase(typeDefine.getDataType())) {
            if (length == null || length <= 0 || length > MAX_RAW_LENGTH) {
                log.warn(
                        "RAW length {} exceeds maximum {}, truncating to {}",
                        length,
                        MAX_RAW_LENGTH,
                        MAX_RAW_LENGTH);
                length = (long) MAX_RAW_LENGTH;
            }
        } else {
            length = Long.MAX_VALUE;
        }
        builder.columnLength(length);
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(YASHAN_NUMBER + "(1)");
                builder.dataType(YASHAN_NUMBER);
                builder.precision(1L);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
                builder.columnType(YASHAN_INTEGER);
                builder.dataType(YASHAN_INTEGER);
                break;
            case BIGINT:
                if (decimalNarrowingEnabled) {
                    builder.columnType(YASHAN_NUMBER + "(19)");
                    builder.dataType(YASHAN_NUMBER);
                    builder.precision(19L);
                } else {
                    builder.columnType(YASHAN_INTEGER);
                    builder.dataType(YASHAN_INTEGER);
                }
                break;
            case FLOAT:
                builder.columnType(YASHAN_FLOAT);
                builder.dataType(YASHAN_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(YASHAN_DOUBLE);
                builder.dataType(YASHAN_DOUBLE);
                break;
            case DECIMAL:
                reconvertDecimalType(column, builder);
                break;
            case BYTES:
                reconvertBytesType(column, builder);
                break;
            case STRING:
                reconvertStringType(column, builder);
                break;
            case DATE:
                builder.columnType(YASHAN_DATE);
                builder.dataType(YASHAN_DATE);
                break;
            case TIMESTAMP:
                reconvertTimestampType(column, builder);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.YASHAN,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void reconvertDecimalType(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        DecimalType decimalType = (DecimalType) column.getDataType();
        long precision =
                decimalType.getPrecision() > 0 ? decimalType.getPrecision() : DEFAULT_PRECISION;
        int scale = decimalType.getScale();

        if (precision > MAX_PRECISION) {
            log.warn(
                    "DECIMAL precision {} exceeds maximum {}, truncating to {}",
                    precision,
                    MAX_PRECISION,
                    MAX_PRECISION);
            precision = MAX_PRECISION;
        }
        if (scale < 0) {
            log.warn("DECIMAL scale {} is negative, setting to 0", scale);
            scale = 0;
        } else if (scale > MAX_SCALE) {
            log.warn(
                    "DECIMAL scale {} exceeds maximum {}, truncating to {}",
                    scale,
                    MAX_SCALE,
                    MAX_SCALE);
            scale = MAX_SCALE;
        }

        builder.columnType(String.format("%s(%d,%d)", YASHAN_NUMBER, precision, scale));
        builder.dataType(YASHAN_NUMBER);
        builder.precision(precision);
        builder.scale(scale);
    }

    private void reconvertBytesType(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        Long length = column.getColumnLength();
        if (length != null && length > 0 && length <= MAX_RAW_LENGTH) {
            builder.columnType(String.format("%s(%d)", YASHAN_RAW, length));
            builder.dataType(YASHAN_RAW);
            builder.length(length);
        } else {
            builder.columnType(YASHAN_BLOB);
            builder.dataType(YASHAN_BLOB);
        }
    }

    private void reconvertStringType(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        Long length = column.getColumnLength();
        if (length == null || length <= 0 || length > MAX_VARCHAR2_LENGTH) {
            builder.columnType(String.format("%s(%d)", YASHAN_VARCHAR2, MAX_VARCHAR2_LENGTH));
            builder.dataType(YASHAN_VARCHAR2);
            builder.length((long) MAX_VARCHAR2_LENGTH);
        } else {
            builder.columnType(String.format("%s(%d)", YASHAN_VARCHAR2, length));
            builder.dataType(YASHAN_VARCHAR2);
            builder.length(length);
        }
    }

    private void reconvertTimestampType(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        Integer scale = column.getScale();
        if (scale == null || scale < 0 || scale > MAX_TIMESTAMP_SCALE) {
            scale = MAX_TIMESTAMP_SCALE;
            log.warn("TIMESTAMP scale {} is invalid, using default {}", column.getScale(), scale);
        }
        builder.columnType(String.format("%s(%d)", YASHAN_TIMESTAMP, scale));
        builder.dataType(YASHAN_TIMESTAMP);
        builder.scale(scale);
    }
}
