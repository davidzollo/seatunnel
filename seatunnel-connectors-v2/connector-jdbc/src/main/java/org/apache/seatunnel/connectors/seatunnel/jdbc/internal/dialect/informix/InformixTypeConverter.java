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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// reference https://www.ibm.com/docs/en/informix-servers/14.10?topic=types-summary-data
@Slf4j
@AutoService(TypeConverter.class)
public class InformixTypeConverter implements TypeConverter<BasicTypeDefine> {
    public static final String BIGINT = "BIGINT";
    public static final String BIGSERIAL = "BIGSERIAL";
    public static final String BSON = "BSON";
    public static final String BYTE = "BYTE";
    public static final String CHAR = "CHAR";
    public static final String CHARACTER = "CHARACTER";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String DEC = "DEC";
    public static final String DECIMAL = "DECIMAL";
    public static final String FLOAT = "FLOAT";
    public static final String INT = "INT";
    public static final String INT8 = "INT8";
    public static final String INTEGER = "INTEGER";
    public static final String MONEY = "MONEY";
    public static final String NCHAR = "NCHAR";
    public static final String NUMERIC = "NUMERIC";
    public static final String NVARCHAR = "NVARCHAR";
    public static final String REAL = "REAL";
    public static final String SERIAL = "SERIAL";
    public static final String SERIAL8 = "SERIAL8";
    public static final String SMALLFLOAT = "SMALLFLOAT";
    public static final String SMALLINT = "SMALLINT";
    public static final String TEXT = "TEXT";
    public static final String VARCHAR = "VARCHAR";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String BLOB = "BLOB";
    public static final String CLOB = "CLOB";
    public static final String LVARCHAR = "LVARCHAR";
    public static final String IDSSECURITYLABEL = "IDSSECURITYLABEL";
    public static final String DOUBLE_PRECISION = "DOUBLE PRECISION";

    public static final int MAX_PRECISION = 32;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int DEFAULT_TIMESTAMP_SCALE = 3;
    public static final int MAX_TIMESTAMP_SCALE = 5;
    public static final int MAX_LVARCHAR_LENGTH = 32739;
    public static final long MAX_TEXT_LENGTH = 2147483648L;
    public static final long MAX_BYTES_LENGTH = 2147483648L;

    public static final InformixTypeConverter INSTANCE = new InformixTypeConverter();

    public static final Pattern p =
            Pattern.compile(
                    "DATETIME\\s+(\\w+)\\s+TO\\s+(\\w+)(?:\\((\\d+)\\))?",
                    Pattern.CASE_INSENSITIVE);

    @Override
    public String identifier() {
        return DatabaseIdentifier.INFORMIX;
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

        String dataType = typeDefine.getDataType().toUpperCase();
        if (dataType.startsWith(DATETIME)) {
            Matcher m = p.matcher(dataType);
            String start = null, end = null;
            int fractionScale = -1;
            if (m.find()) {
                start = m.group(1).toUpperCase();
                end = m.group(2).toUpperCase();
                if ("FRACTION".equals(end) && m.group(3) != null) {
                    fractionScale = Integer.parseInt(m.group(3));
                }
            }

            List<String> dateFields = Arrays.asList("YEAR", "MONTH", "DAY");
            List<String> timeFields = Arrays.asList("HOUR", "MINUTE", "SECOND", "FRACTION");

            boolean hasDate = dateFields.contains(start);
            boolean hasTime = timeFields.contains(end);

            if (hasDate && !hasTime) {
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
            } else if (!hasDate && hasTime) {
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
            } else {
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
            }

            if (fractionScale > 0) {
                builder.scale(fractionScale);
            }
            return builder.build();
        }
        switch (dataType) {
            case BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case INT:
            case INTEGER:
            case SERIAL:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case BIGINT:
            case INT8:
            case BIGSERIAL:
            case SERIAL8:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case FLOAT:
            case REAL:
            case SMALLFLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DEC:
            case DECIMAL:
            case MONEY:
            case NUMERIC:
                long precision = typeDefine.getPrecision();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                } else if (precision > MAX_PRECISION) {
                    precision = MAX_PRECISION;
                }
                int scale = typeDefine.getScale();
                if (scale < 0 || scale > precision) {
                    scale = 0;
                }
                builder.dataType(new DecimalType(Math.toIntExact(precision), scale));
                builder.sourceType(String.format("%s(%s,%s)", DECIMAL, precision, scale));
                break;
            case CHAR:
            case CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.sourceType(String.format("%s(%s)", CHAR, typeDefine.getLength()));
                break;
            case NCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.sourceType(String.format("%s(%s)", NCHAR, typeDefine.getLength()));
                break;
            case NVARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.sourceType(String.format("%s(%s)", NVARCHAR, typeDefine.getLength()));
                break;
            case VARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.sourceType(String.format("%s(%s)", VARCHAR, typeDefine.getLength()));
                break;
            case LVARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.sourceType(String.format("%s(%s)", LVARCHAR, typeDefine.getLength()));
                break;
            case TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                builder.sourceType(TEXT);
                break;
            case IDSSECURITYLABEL:
            case CLOB:
            case BSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case BYTE:
            case BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.INFORMIX, dataType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<MysqlType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.<MysqlType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(BOOLEAN);
                builder.dataType(BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(SMALLINT);
                builder.dataType(SMALLINT);
                break;
            case INT:
                builder.columnType(INT);
                builder.dataType(INT);
                break;
            case BIGINT:
                builder.columnType(BIGINT);
                builder.dataType(BIGINT);
                break;
            case FLOAT:
                builder.columnType(FLOAT);
                builder.dataType(FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DOUBLE_PRECISION);
                builder.dataType(DOUBLE_PRECISION);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_SCALE,
                            precision,
                            scale);
                }
                builder.columnType(String.format("%s(%s,%s)", DECIMAL, precision, scale));
                builder.dataType(DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(BYTE);
                    builder.dataType(BYTE);
                } else if (column.getColumnLength() < MAX_BYTES_LENGTH) {
                    builder.columnType(BYTE);
                    builder.dataType(BYTE);
                } else {
                    builder.columnType(BLOB);
                    builder.dataType(BLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(TEXT);
                    builder.dataType(TEXT);
                } else if (column.getColumnLength() < MAX_LVARCHAR_LENGTH) {
                    builder.columnType(String.format("%s(%s)", LVARCHAR, column.getColumnLength()));
                    builder.dataType(LVARCHAR);
                } else if (column.getColumnLength() < MAX_TEXT_LENGTH) {
                    builder.columnType(TEXT);
                    builder.dataType(TEXT);
                } else {
                    builder.columnType(CLOB);
                    builder.dataType(CLOB);
                }
                break;
            case DATE:
                builder.columnType(DATE);
                builder.dataType(DATE);
                break;
            case TIMESTAMP:
                builder.dataType(DATETIME);
                if (column.getScale() == null || column.getScale() <= DEFAULT_TIMESTAMP_SCALE) {
                    builder.columnType("DATETIME YEAR TO FRACTION");
                    builder.scale(column.getScale());
                } else if (column.getScale() <= MAX_TIMESTAMP_SCALE) {
                    builder.columnType(
                            String.format("DATETIME YEAR TO FRACTION(%s)", column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(
                            String.format("DATETIME YEAR TO FRACTION(%s)", MAX_TIMESTAMP_SCALE));
                    builder.scale(MAX_TIMESTAMP_SCALE);
                    log.warn(
                            "The timestamp column {} type timestamp({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to timestamp({})",
                            column.getName(),
                            column.getScale(),
                            MAX_TIMESTAMP_SCALE,
                            MAX_TIMESTAMP_SCALE);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.INFORMIX,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}
