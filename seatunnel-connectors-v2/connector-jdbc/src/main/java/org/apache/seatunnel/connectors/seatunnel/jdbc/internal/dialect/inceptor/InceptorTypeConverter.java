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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference
// https://docs.transwarp.cn/#/documents-support/docs-detail/document/TDH-PLATFORM/9.3/010Inceptor-Developer-Guide?docType=docs%3Fcategory%3DTDH&docName=Inceptor%E4%BD%BF%E7%94%A8%E6%89%8B%E5%86%8C
@Slf4j
@AutoService(TypeConverter.class)
public class InceptorTypeConverter implements TypeConverter<BasicTypeDefine> {
    // Numeric Types
    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String INT = "INT";
    private static final String INTEGER = "INTEGER";
    private static final String BIGINT = "BIGINT";
    private static final String FLOAT = "FLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DECIMAL = "DECIMAL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMBER = "NUMBER";
    // Date/Time Types
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String INTERVAL = "INTERVAL";
    // String Types
    private static final String STRING = "STRING";
    private static final String VARCHAR = "VARCHAR";
    private static final String VARCHAR2 = "VARCHAR2";
    private static final String CHAR = "CHAR";
    // Misc Types
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BLOB = "BLOB";
    private static final String CLOB = "CLOB";
    // Complex Types
    private static final String ARRAY = "ARRAY";
    private static final String MAP = "MAP";
    // float
    private static final String STRUCT = "STRUCT";
    private static final String UNIONTYPE = "UNIONTYPE";

    public static final InceptorTypeConverter INSTANCE = new InceptorTypeConverter();
    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 38;
    public static final int DEFAULT_SCALE = 18;

    public static final long KB_4 = 4096L;

    @Override
    public String identifier() {
        return DatabaseIdentifier.INCEPTOR;
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
        switch (dataType) {
            case TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case INT:
            case INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DOUBLE:
            case DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DECIMAL:
            case NUMBER:
            case NUMERIC:
                builder.dataType(
                        new DecimalType(
                                Math.toIntExact(
                                        typeDefine.getPrecision() == null
                                                ? DEFAULT_PRECISION
                                                : typeDefine.getPrecision()),
                                typeDefine.getScale() == null
                                        ? DEFAULT_SCALE
                                        : typeDefine.getScale()));
                break;
            case TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(9);
                break;
            case DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case STRING:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case VARCHAR:
            case VARCHAR2:
            case CHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case CLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case ARRAY:
            case INTERVAL:
            case MAP:
            case STRUCT:
            case UNIONTYPE:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.INCEPTOR,
                        PluginType.SOURCE,
                        dataType,
                        typeDefine.getName());
        }
        return builder.build();
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
            case ARRAY:
            case MAP:
            case ROW:
            case NULL:
                builder.columnType(STRING);
                builder.dataType(STRING);
                break;
            case STRING:
                if (column.getColumnLength() != null) {
                    if (column.getColumnLength() > KB_4) {
                        builder.columnType(CLOB);
                        builder.dataType(CLOB);
                    } else if (column.getColumnLength() > 0) {
                        builder.columnType(
                                String.format("%s(%s)", VARCHAR2, column.getColumnLength()));
                        builder.dataType(VARCHAR2);
                    } else {
                        builder.columnType(STRING);
                        builder.dataType(STRING);
                    }
                } else {
                    builder.columnType(STRING);
                    builder.dataType(STRING);
                }
                builder.length(column.getColumnLength());
                break;
            case BOOLEAN:
                builder.columnType(BOOLEAN);
                builder.dataType(BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(TINYINT);
                builder.dataType(TINYINT);
                break;
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
                builder.columnType(DOUBLE);
                builder.dataType(DOUBLE);
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
                builder.columnType(BLOB);
                builder.dataType(BLOB);
                break;
            case DATE:
                builder.columnType(DATE);
                builder.dataType(DATE);
                break;
            case TIME:
                builder.columnType(TIME);
                builder.dataType(TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() > 9) {
                    log.warn(
                            "The timestamp column {} type timestamp({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to timestamp({})",
                            column.getName(),
                            column.getScale(),
                            9,
                            9);
                }
                builder.columnType(TIMESTAMP);
                builder.dataType(TIMESTAMP);
                builder.scale(9);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.INCEPTOR,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
