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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@AutoService(TypeConverter.class)
public class JdbcStarRocksTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String STARROCKS_NULL = "NULL";

    public static final String STARROCKS_BIT = "BIT";
    public static final String STARROCKS_BOOLEAN = "BOOLEAN";

    public static final String STARROCKS_LARGEINT = "LARGEINT";
    public static final String STARROCKS_TINYINT = "TINYINT";
    public static final String STARROCKS_SMALLINT = "SMALLINT";
    public static final String STARROCKS_MEDIUMINT = "MEDIUMINT";
    public static final String STARROCKS_INT = "INT";
    public static final String STARROCKS_INT_UNSIGNED = "INT UNSIGNED";
    public static final String STARROCKS_INTEGER = "INTEGER";
    public static final String STARROCKS_BIGINT = "BIGINT";
    public static final String STARROCKS_DECIMAL = "DECIMAL";
    public static final String STARROCKS_FLOAT = "FLOAT";
    public static final String STARROCKS_DOUBLE = "DOUBLE";

    public static final String STARROCKS_DATE = "DATE";
    public static final String STARROCKS_DATETIME = "DATETIME";
    public static final String STARROCKS_YEAR = "YEAR";

    public static final String STARROCKS_CHAR = "CHAR";
    public static final String STARROCKS_VARCHAR = "VARCHAR";
    public static final String STARROCKS_STRING = "STRING";
    public static final String STARROCKS_TINYTEXT = "TINYTEXT";
    public static final String STARROCKS_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String STARROCKS_TEXT = "TEXT";
    public static final String STARROCKS_LONGTEXT = "LONGTEXT";
    public static final String STARROCKS_JSON = "JSON";
    public static final String STARROCKS_ENUM = "ENUM";

    public static final String STARROCKS_BOOLEAN_ARRAY = "ARRAY<boolean>";
    public static final String STARROCKS_TINYINT_ARRAY = "ARRAY<tinyint>";
    public static final String STARROCKS_SMALLINT_ARRAY = "ARRAY<smallint>";
    public static final String STARROCKS_INT_ARRAY = "ARRAY<int>";
    public static final String STARROCKS_BIGINT_ARRAY = "ARRAY<bigint>";
    public static final String STARROCKS_FLOAT_ARRAY = "ARRAY<float>";
    public static final String STARROCKS_DOUBLE_ARRAY = "ARRAY<double>";
    public static final String STARROCKS_STRING_ARRAY = "ARRAY<STRING>";
    public static final String STARROCKS_DATE_ARRAY = "ARRAY<DATE>";
    public static final String STARROCKS_DATETIME_ARRAY = "ARRAY<DATETIME>";

    public static final String STARROCKS_ARRAY = "ARRAY";
    public static final String STARROCKS_ARRAY_BOOLEAN_INTER = "tinyint(1)";
    public static final String STARROCKS_ARRAY_TINYINT_INTER = "tinyint(4)";
    public static final String STARROCKS_ARRAY_SMALLINT_INTER = "smallint(6)";
    public static final String STARROCKS_ARRAY_INT_INTER = "int(11)";
    public static final String STARROCKS_ARRAY_BIGINT_INTER = "bigint(20)";
    public static final String STARROCKS_ARRAY_DECIMAL_PRE = "DECIMAL";
    public static final String STARROCKS_ARRAY_DATE_INTER = "date";
    public static final String STARROCKS_ARRAY_DATETIME_INTER = "DATETIME";

    public static final String STARROCKS_MAP = "MAP";
    public static final String STARROCKS_MAP_COLUMN_TYPE = "MAP<%s, %s>";

    public static final Long DEFAULT_PRECISION = 9L;
    public static final Long MAX_PRECISION = 38L;

    public static final Integer DEFAULT_SCALE = 0;
    public static final Integer MAX_SCALE = 10;

    public static final Integer MAX_DATETIME_SCALE = 6;

    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long POWER_2_24 = (long) Math.pow(2, 24);
    public static final long POWER_2_32 = (long) Math.pow(2, 32);

    // Min value of LARGEINT is -170141183460469231731687303715884105728, it will use 39 bytes in
    // UTF-8.
    // Add a bit to prevent overflow
    public static final long MAX_STARROCKS_LARGEINT_TO_VARCHAR_LENGTH = 39L;

    public static final long MAX_VARCHAR_LENGTH = 65533;
    public static final long MAX_STRING_LENGTH = 2147483643;

    public static final JdbcStarRocksTypeConverter INSTANCE = new JdbcStarRocksTypeConverter();

    private PhysicalColumn.PhysicalColumnBuilder getPhysicalColumnBuilder(
            BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        return builder;
    }

    private BasicTypeDefine.BasicTypeDefineBuilder getBasicTypeDefineBuilder(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        return builder;
    }

    private String getStarRocksColumnName(String starRocksColumnType) {
        starRocksColumnType = starRocksColumnType.toUpperCase(Locale.ROOT);
        int idx = starRocksColumnType.indexOf("(");
        int idx2 = starRocksColumnType.indexOf("<");
        if (idx != -1) {
            starRocksColumnType = starRocksColumnType.substring(0, idx);
        }
        if (idx2 != -1) {
            starRocksColumnType = starRocksColumnType.substring(0, idx2);
        }
        return starRocksColumnType;
    }

    private String getStarRocksColumnName(BasicTypeDefine typeDefine) {
        String starRocksColumnType = typeDefine.getColumnType();
        return getStarRocksColumnName(starRocksColumnType);
    }

    public void sampleTypeConverter(
            PhysicalColumn.PhysicalColumnBuilder builder,
            BasicTypeDefine typeDefine,
            String starRocksColumnType) {
        switch (starRocksColumnType) {
            case STARROCKS_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case STARROCKS_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case STARROCKS_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else if (typeDefine.getLength() == 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    // BIT(M) -> BYTE(M/8)
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                }
                break;
            case STARROCKS_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case STARROCKS_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case STARROCKS_MEDIUMINT:
            case STARROCKS_INT:
            case STARROCKS_INTEGER:
            case STARROCKS_YEAR:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case STARROCKS_INT_UNSIGNED:
            case STARROCKS_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case STARROCKS_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case STARROCKS_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case STARROCKS_ENUM:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(100L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case STARROCKS_CHAR:
            case STARROCKS_VARCHAR:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case STARROCKS_LARGEINT:
                DecimalType decimalType;
                decimalType = new DecimalType(20, 0);
                builder.dataType(decimalType);
                builder.columnLength(20L);
                builder.scale(0);
                break;
            case STARROCKS_STRING:
            case STARROCKS_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_STRING_LENGTH);
                break;
            case STARROCKS_TINYTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_8 - 1);
                break;
            case STARROCKS_TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_16 - 1);
                break;
            case STARROCKS_MEDIUMTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_24 - 1);
                break;
            case STARROCKS_LONGTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_32 - 1);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.STARROCKS, starRocksColumnType, typeDefine.getName());
        }
    }

    private BasicTypeDefine sampleReconvert(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(STARROCKS_NULL);
                builder.dataType(STARROCKS_NULL);
                break;
            case BYTES:
                builder.columnType(STARROCKS_STRING);
                builder.dataType(STARROCKS_STRING);
                break;
            case BOOLEAN:
                builder.columnType(STARROCKS_BOOLEAN);
                builder.dataType(STARROCKS_BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.columnType(STARROCKS_TINYINT);
                builder.dataType(STARROCKS_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(STARROCKS_SMALLINT);
                builder.dataType(STARROCKS_SMALLINT);
                break;
            case INT:
                builder.columnType(STARROCKS_INT);
                builder.dataType(STARROCKS_INT);
                break;
            case BIGINT:
                builder.columnType(STARROCKS_BIGINT);
                builder.dataType(STARROCKS_BIGINT);
                break;
            case FLOAT:
                builder.columnType(STARROCKS_FLOAT);
                builder.dataType(STARROCKS_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(STARROCKS_DOUBLE);
                builder.dataType(STARROCKS_DOUBLE);
                break;
            case DECIMAL:
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(STARROCKS_LARGEINT)) {
                    builder.dataType(STARROCKS_LARGEINT);
                    builder.columnType(STARROCKS_LARGEINT);
                    break;
                }
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = MAX_PRECISION.intValue();
                    scale = MAX_SCALE;
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
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to varchar(200)",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION);
                    builder.dataType(STARROCKS_VARCHAR);
                    builder.columnType(String.format("%s(%s)", STARROCKS_VARCHAR, 200));
                    break;
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
                } else if (scale > precision) {
                    scale = precision;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            precision,
                            scale);
                }

                builder.columnType(String.format("%s(%s,%s)", STARROCKS_DECIMAL, precision, scale));
                builder.dataType(STARROCKS_DECIMAL);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.length(8L);
                builder.columnType(String.format("%s(%s)", STARROCKS_VARCHAR, 8));
                builder.dataType(STARROCKS_VARCHAR);
                break;
            case ARRAY:
                SeaTunnelDataType<?> dataType = column.getDataType();
                SeaTunnelDataType elementType = null;
                if (dataType instanceof ArrayType) {
                    ArrayType arrayType = (ArrayType) dataType;
                    elementType = arrayType.getElementType();
                }

                reconvertBuildArrayInternal(elementType, builder, column.getName());
                break;
            case ROW:
                builder.columnType(STARROCKS_JSON);
                builder.dataType(STARROCKS_JSON);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.STARROCKS,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void reconvertBuildArrayInternal(
            SeaTunnelDataType elementType,
            BasicTypeDefine.BasicTypeDefineBuilder builder,
            String columnName) {
        switch (elementType.getSqlType()) {
            case BOOLEAN:
                builder.columnType(STARROCKS_BOOLEAN_ARRAY);
                builder.dataType(STARROCKS_BOOLEAN_ARRAY);
                break;
            case TINYINT:
                builder.columnType(STARROCKS_TINYINT_ARRAY);
                builder.dataType(STARROCKS_TINYINT_ARRAY);
                break;
            case SMALLINT:
                builder.columnType(STARROCKS_SMALLINT_ARRAY);
                builder.dataType(STARROCKS_SMALLINT_ARRAY);
                break;
            case INT:
                builder.columnType(STARROCKS_INT_ARRAY);
                builder.dataType(STARROCKS_INT_ARRAY);
                break;
            case BIGINT:
                builder.columnType(STARROCKS_BIGINT_ARRAY);
                builder.dataType(STARROCKS_BIGINT_ARRAY);
                break;
            case FLOAT:
                builder.columnType(STARROCKS_FLOAT_ARRAY);
                builder.dataType(STARROCKS_FLOAT_ARRAY);
                break;
            case DOUBLE:
                builder.columnType(STARROCKS_DOUBLE_ARRAY);
                builder.dataType(STARROCKS_DOUBLE_ARRAY);
                break;
            case STRING:
            case TIME:
                builder.columnType(STARROCKS_STRING_ARRAY);
                builder.dataType(STARROCKS_STRING_ARRAY);
                break;
            case DATE:
                builder.columnType(STARROCKS_DATE_ARRAY);
                builder.dataType(STARROCKS_DATE_ARRAY);
                break;
            case TIMESTAMP:
                builder.columnType(STARROCKS_DATETIME_ARRAY);
                builder.dataType(STARROCKS_DATETIME_ARRAY);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.STARROCKS, elementType.getSqlType().name(), columnName);
        }
    }

    private static int[] getPrecisionAndScale(String decimalTypeDefinition) {
        decimalTypeDefinition = decimalTypeDefinition.toUpperCase(Locale.ROOT);
        String numericPart = decimalTypeDefinition.replace("DECIMAL(", "").replace(")", "");
        numericPart = numericPart.replace("DECIMAL(", "").replace(")", "");
        // Split by comma to separate precision and scale
        String[] parts = numericPart.split(",");

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid DECIMAL definition: " + decimalTypeDefinition);
        }
        // Parse precision and scale from the split parts
        int precision = Integer.parseInt(parts[0].trim());
        int scale = Integer.parseInt(parts[1].trim());
        // Return an array containing precision and scale
        return new int[] {precision, scale};
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder = getPhysicalColumnBuilder(typeDefine);
        String starRocksColumnType = getStarRocksColumnName(typeDefine);

        if (starRocksColumnType.toUpperCase(Locale.ROOT).startsWith(STARROCKS_ARRAY_DECIMAL_PRE)) {
            Long p = MAX_PRECISION;
            int scale = MAX_SCALE;
            if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                p = typeDefine.getPrecision();
            }

            if (typeDefine.getScale() != null && typeDefine.getScale() > 0) {
                scale = typeDefine.getScale();
            }
            DecimalType decimalType;
            decimalType = new DecimalType(p.intValue(), scale);
            builder.dataType(decimalType);
            builder.columnLength(p);
            builder.scale(scale);
        } else {
            switch (starRocksColumnType) {
                case STARROCKS_DATE:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                    break;
                case STARROCKS_DATETIME:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                    builder.scale(typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                    break;
                case STARROCKS_ARRAY:
                    convertArray(typeDefine.getColumnType(), builder, typeDefine.getName());
                    break;
                case STARROCKS_MAP:
                    convertMap(typeDefine.getColumnType(), builder, typeDefine.getName());
                    break;
                default:
                    sampleTypeConverter(builder, typeDefine, starRocksColumnType);
            }
        }

        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder = getBasicTypeDefineBuilder(column);

        switch (column.getDataType().getSqlType()) {
            case STRING:
                reconvertString(column, builder);
                break;
            case DATE:
                builder.columnType(STARROCKS_DATE);
                builder.dataType(STARROCKS_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() >= 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(
                            String.format("%s(%s)", STARROCKS_DATETIME, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(
                            String.format("%s(%s)", STARROCKS_DATETIME, MAX_DATETIME_SCALE));
                    builder.scale(MAX_DATETIME_SCALE);
                }
                builder.dataType(STARROCKS_DATETIME);
                break;
            case MAP:
                reconvertMap(column, builder);
                break;
            default:
                sampleReconvert(column, builder);
        }
        return builder.build();
    }

    private void convertMap(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String[] keyValueType = extractMapKeyValueType(columnType);
        MapType mapType =
                new MapType(
                        turnColumnTypeToSeaTunnelType(keyValueType[0], name + ".key"),
                        turnColumnTypeToSeaTunnelType(keyValueType[1], name + ".value"));
        builder.dataType(mapType);
    }

    private SeaTunnelDataType turnColumnTypeToSeaTunnelType(String columnType, String columnName) {
        BasicTypeDefine keyBasicTypeDefine =
                BasicTypeDefine.<MysqlType>builder()
                        .columnType(columnType)
                        .name(columnName)
                        .build();
        if (columnType.toUpperCase(Locale.ROOT).startsWith(STARROCKS_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnType);
            keyBasicTypeDefine.setPrecision(Long.valueOf(precisionAndScale[0]));
            keyBasicTypeDefine.setScale(precisionAndScale[1]);
        }
        Column column = convert(keyBasicTypeDefine);
        return column.getDataType();
    }

    private void convertArray(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String columnInterType = extractArrayType(columnType);
        if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_BOOLEAN_INTER)) {
            builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_TINYINT_INTER)) {
            builder.dataType(ArrayType.BYTE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_SMALLINT_INTER)) {
            builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_INT_INTER)) {
            builder.dataType(ArrayType.INT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_BIGINT_INTER)) {
            builder.dataType(ArrayType.LONG_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_FLOAT)) {
            builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_DOUBLE)) {
            builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith("CHAR")
                || columnInterType.toUpperCase(Locale.ROOT).startsWith("VARCHAR")
                || columnInterType.equalsIgnoreCase(STARROCKS_STRING)) {
            builder.dataType(ArrayType.STRING_ARRAY_TYPE);
        } else if (columnInterType
                .toUpperCase(Locale.ROOT)
                .startsWith(STARROCKS_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnInterType);
            DecimalArrayType decimalArray =
                    new DecimalArrayType(
                            new DecimalType(precisionAndScale[0], precisionAndScale[1]));
            builder.dataType(decimalArray);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_DATE_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_ARRAY_DATETIME_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(STARROCKS_LARGEINT)) {
            DecimalArrayType decimalArray = new DecimalArrayType(new DecimalType(20, 0));
            builder.dataType(decimalArray);
        } else {
            throw CommonError.convertToSeaTunnelTypeError(
                    DatabaseIdentifier.STARROCKS, columnType, name);
        }
    }

    private static String extractArrayType(String input) {
        Pattern pattern = Pattern.compile("<(.*?)>");
        Matcher matcher = pattern.matcher(input);
        return matcher.find() ? matcher.group(1) : "";
    }

    private static String[] extractMapKeyValueType(String input) {
        String[] result = new String[2];
        input = input.replaceAll("map<", "").replaceAll("MAP<", "").replaceAll(">", "");
        String[] split = input.split(",");
        if (split.length == 4) {
            // decimal(10,2),decimal(10,2)
            result[0] = split[0] + "," + split[1];
            result[1] = split[2] + "," + split[3];
        } else if (split.length == 3) {
            // decimal(10,2), date
            // decimal(10, 2), varchar(20)
            if (split[0].indexOf("(") != -1 && split[1].indexOf(")") != -1) {
                result[0] = split[0] + "," + split[1];
                result[1] = split[2];
            } else if (split[1].indexOf("(") != -1 && split[2].indexOf(")") != -1) {
                // date, decimal(10, 2)
                // varchar(20), decimal(10, 2)
                result[0] = split[0];
                result[1] = split[1] + "," + split[2];
            } else {
                return null;
            }
        } else if (split.length == 2) {
            result[0] = split[0];
            result[1] = split[1];
        } else {
            return null;
        }
        return result;
    }

    private void reconvertMap(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        MapType dataType = (MapType) column.getDataType();
        SeaTunnelDataType keyType = dataType.getKeyType();
        SeaTunnelDataType valueType = dataType.getValueType();
        Column keyColumn =
                PhysicalColumn.of(
                        column.getName() + ".key",
                        (SeaTunnelDataType<?>) keyType,
                        (Long) null,
                        true,
                        null,
                        null);
        String keyColumnType = reconvert(keyColumn).getColumnType();

        Column valueColumn =
                PhysicalColumn.of(
                        column.getName() + ".value",
                        (SeaTunnelDataType<?>) valueType,
                        (Long) null,
                        true,
                        null,
                        null);
        String valueColumnType = reconvert(valueColumn).getColumnType();

        builder.dataType(String.format(STARROCKS_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
        builder.columnType(
                String.format(STARROCKS_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        // source is starRocks too.
        if (column.getSourceType() != null
                && column.getSourceType().equalsIgnoreCase(STARROCKS_JSON)) {
            builder.columnType(STARROCKS_JSON);
            builder.dataType(STARROCKS_JSON);
            return;
        }
        sampleReconvertString(column, builder);
    }

    private void sampleReconvertString(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
            builder.columnType(STARROCKS_STRING);
            builder.dataType(STARROCKS_STRING);
            return;
        }

        if (column.getColumnLength() < POWER_2_8) {
            if (column.getSourceType() != null
                    && column.getSourceType()
                            .toUpperCase(Locale.ROOT)
                            .startsWith(STARROCKS_VARCHAR)) {
                builder.columnType(
                        String.format("%s(%s)", STARROCKS_VARCHAR, column.getColumnLength()));
                builder.dataType(STARROCKS_VARCHAR);
            } else {
                builder.columnType(
                        String.format("%s(%s)", STARROCKS_CHAR, column.getColumnLength()));
                builder.dataType(STARROCKS_CHAR);
            }
            return;
        }

        if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
            builder.columnType(
                    String.format("%s(%s)", STARROCKS_VARCHAR, column.getColumnLength()));
            builder.dataType(STARROCKS_VARCHAR);
            return;
        }

        if (column.getColumnLength() <= MAX_STRING_LENGTH) {
            builder.columnType(STARROCKS_STRING);
            builder.dataType(STARROCKS_STRING);
            return;
        }

        if (column.getColumnLength() > MAX_STRING_LENGTH) {
            log.warn(
                    String.format(
                            "The String type in StarRocks can only store up to 2GB bytes, and the current field [%s] length is [%s] bytes. If it is greater than the maximum length of the String in StarRocks, it may not be able to write data",
                            column.getName(), column.getColumnLength()));
            builder.columnType(STARROCKS_STRING);
            builder.dataType(STARROCKS_STRING);
            return;
        }
        throw CommonError.convertToConnectorTypeError(
                DatabaseIdentifier.STARROCKS,
                column.getDataType().getSqlType().name(),
                column.getName());
    }

    @Override
    public String identifier() {
        return DatabaseIdentifier.STARROCKS;
    }
}
