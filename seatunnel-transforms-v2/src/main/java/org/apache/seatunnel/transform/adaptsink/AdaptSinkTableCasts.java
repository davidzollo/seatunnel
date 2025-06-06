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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public class AdaptSinkTableCasts {
    private static final DateTimeFormatter BASIC_DATE_TIME =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(
                            new DateTimeFormatterBuilder()
                                    .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                    .appendValue(MONTH_OF_YEAR, 2)
                                    .appendValue(DAY_OF_MONTH, 2)
                                    .toFormatter())
                    .optionalStart()
                    .appendLiteral(' ')
                    .append(
                            new DateTimeFormatterBuilder()
                                    .appendValue(HOUR_OF_DAY, 2)
                                    .appendLiteral(':')
                                    .appendValue(MINUTE_OF_HOUR, 2)
                                    .optionalStart()
                                    .appendLiteral(':')
                                    .appendValue(SECOND_OF_MINUTE, 2)
                                    .optionalStart()
                                    .appendFraction(NANO_OF_SECOND, 0, 9, true)
                                    .toFormatter())
                    .toFormatter();

    public static void tryCastColumnType(Column inputColumn, Column outputColumn) {
        try {
            castColumnData(inputColumn, outputColumn, "test");
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException) {
                throw e;
            }
        }
    }

    public static Object castColumnData(
            Column inputColumn, Column outputColumn, Object columnValue) {
        if (columnValue == null) {
            return null;
        }
        switch (outputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return AdaptSinkTableCasts.castAsBoolean(inputColumn, columnValue);
            case STRING:
                return AdaptSinkTableCasts.castAsString(inputColumn, columnValue);
            case TINYINT:
                return AdaptSinkTableCasts.castAsByte(inputColumn, columnValue);
            case SMALLINT:
                return AdaptSinkTableCasts.castAsShort(inputColumn, columnValue);
            case INT:
                return AdaptSinkTableCasts.castAsInt(inputColumn, columnValue);
            case BIGINT:
                return AdaptSinkTableCasts.castAsLong(inputColumn, columnValue);
            case FLOAT:
                return AdaptSinkTableCasts.castAsFloat(inputColumn, columnValue);
            case DOUBLE:
                return AdaptSinkTableCasts.castAsDouble(inputColumn, columnValue);
            case DECIMAL:
                return AdaptSinkTableCasts.castAsDecimal(inputColumn, outputColumn, columnValue);
            case BYTES:
                return AdaptSinkTableCasts.castAsBytes(inputColumn, columnValue);
            case DATE:
                return AdaptSinkTableCasts.castAsDate(inputColumn, columnValue);
            case TIME:
                return AdaptSinkTableCasts.castAsTime(inputColumn, columnValue);
            case TIMESTAMP:
                return AdaptSinkTableCasts.castAsTimestamp(inputColumn, columnValue);
            case ARRAY:
                return AdaptSinkTableCasts.castAsArray(inputColumn, columnValue);
            case MAP:
                return AdaptSinkTableCasts.castAsMap(inputColumn, columnValue);
            case ROW:
                return AdaptSinkTableCasts.castAsRow(inputColumn, columnValue);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static boolean castAsBoolean(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return false;
            case BOOLEAN:
                return (boolean) columnValue;
            case STRING:
                return Boolean.parseBoolean((String) columnValue);
            case TINYINT:
                return (byte) columnValue != 0;
            case SMALLINT:
                return (short) columnValue != 0;
            case INT:
                return (int) columnValue != 0;
            case BIGINT:
                return (long) columnValue != 0;
            case FLOAT:
                return (float) columnValue != 0;
            case DOUBLE:
                return (double) columnValue != 0;
            case DECIMAL:
                return ((BigDecimal) columnValue).longValue() != 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Byte castAsByte(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (byte) ((boolean) columnValue ? 1 : 0);
            case STRING:
                return Byte.parseByte((String) columnValue);
            case TINYINT:
                return (byte) columnValue;
            case SMALLINT:
                return (byte) (short) columnValue;
            case INT:
                return (byte) (int) columnValue;
            case BIGINT:
                return (byte) (long) columnValue;
            case FLOAT:
                return (byte) (float) columnValue;
            case DOUBLE:
                return (byte) (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).byteValue();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Short castAsShort(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (short) ((boolean) columnValue ? 1 : 0);
            case STRING:
                return Short.parseShort((String) columnValue);
            case TINYINT:
                return (short) (byte) columnValue;
            case SMALLINT:
                return (short) columnValue;
            case INT:
                return (short) (int) columnValue;
            case BIGINT:
                return (short) (long) columnValue;
            case FLOAT:
                return (short) (float) columnValue;
            case DOUBLE:
                return (short) (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).shortValue();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Integer castAsInt(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (boolean) columnValue ? 1 : 0;
            case STRING:
                return Integer.parseInt((String) columnValue);
            case TINYINT:
                return (int) (byte) columnValue;
            case SMALLINT:
                return (int) (short) columnValue;
            case INT:
                return (int) columnValue;
            case BIGINT:
                return (int) (long) columnValue;
            case FLOAT:
                return (int) (float) columnValue;
            case DOUBLE:
                return (int) (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).intValue();
            case TIME:
                return ((LocalTime) columnValue).toSecondOfDay();
            case DATE:
                return (int) ((LocalDate) columnValue).toEpochDay();
            case TIMESTAMP:
                return (int) ((LocalDateTime) columnValue).toEpochSecond(ZoneOffset.UTC);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Long castAsLong(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (boolean) columnValue ? 1L : 0L;
            case STRING:
                return Long.parseLong((String) columnValue);
            case TINYINT:
                return (long) (byte) columnValue;
            case SMALLINT:
                return (long) (short) columnValue;
            case INT:
                return (long) (int) columnValue;
            case BIGINT:
                return (long) columnValue;
            case FLOAT:
                return (long) (float) columnValue;
            case DOUBLE:
                return (long) (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).longValue();
            case TIME:
                return ((LocalTime) columnValue).toNanoOfDay();
            case DATE:
                return ((LocalDate) columnValue).toEpochDay();
            case TIMESTAMP:
                return ((LocalDateTime) columnValue)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Float castAsFloat(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (boolean) columnValue ? 1.0f : 0.0f;
            case STRING:
                return Float.parseFloat((String) columnValue);
            case TINYINT:
                return (float) (byte) columnValue;
            case SMALLINT:
                return (float) (short) columnValue;
            case INT:
                return (float) (int) columnValue;
            case BIGINT:
                return (float) (long) columnValue;
            case FLOAT:
                return (float) columnValue;
            case DOUBLE:
                return (float) (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).floatValue();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Double castAsDouble(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return (boolean) columnValue ? 1.0 : 0.0;
            case STRING:
                return Double.parseDouble((String) columnValue);
            case TINYINT:
                return (double) (byte) columnValue;
            case SMALLINT:
                return (double) (short) columnValue;
            case INT:
                return (double) (int) columnValue;
            case BIGINT:
                return (double) (long) columnValue;
            case FLOAT:
                return (double) (float) columnValue;
            case DOUBLE:
                return (double) columnValue;
            case DECIMAL:
                return ((BigDecimal) columnValue).doubleValue();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static BigDecimal castAsDecimal(
            Column inputColumn, Column outputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return BigDecimal.valueOf((boolean) columnValue ? 1 : 0);
            case STRING:
                return new BigDecimal((String) columnValue);
            case TINYINT:
                return BigDecimal.valueOf((byte) columnValue);
            case SMALLINT:
                return BigDecimal.valueOf((short) columnValue);
            case INT:
                return BigDecimal.valueOf((int) columnValue);
            case BIGINT:
                return BigDecimal.valueOf((long) columnValue);
            case FLOAT:
                return BigDecimal.valueOf((float) columnValue);
            case DOUBLE:
                return BigDecimal.valueOf((double) columnValue);
            case DECIMAL:
                DecimalType outputType = (DecimalType) outputColumn.getDataType();
                DecimalType inputType = (DecimalType) inputColumn.getDataType();
                if (outputType.getScale() != inputType.getScale()) {
                    BigDecimal value = (BigDecimal) columnValue;
                    return value.setScale(outputType.getScale(), RoundingMode.DOWN);
                }
                return (BigDecimal) columnValue;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static byte[] castAsBytes(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BYTES:
                return (byte[]) columnValue;
            case STRING:
                return ((String) columnValue).getBytes();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static String castAsString(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case STRING:
                return (String) columnValue;
            case BOOLEAN:
                return String.valueOf((boolean) columnValue);
            case TINYINT:
                return String.valueOf((byte) columnValue);
            case SMALLINT:
                return String.valueOf((short) columnValue);
            case INT:
                return String.valueOf((int) columnValue);
            case BIGINT:
                return String.valueOf((long) columnValue);
            case FLOAT:
                return String.valueOf((float) columnValue);
            case DOUBLE:
                return String.valueOf((double) columnValue);
            case DECIMAL:
                return ((BigDecimal) columnValue).toPlainString();
            case DATE:
                return ((LocalDate) columnValue).format(DateTimeFormatter.ISO_DATE);
            case TIME:
                return ((LocalTime) columnValue).format(DateTimeFormatter.ISO_TIME);
            case TIMESTAMP:
                return ((LocalDateTime) columnValue).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case MAP:
            case ARRAY:
            case ROW:
                return String.valueOf(columnValue);
            case BYTES:
                return new String((byte[]) columnValue);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static LocalDate castAsDate(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case DATE:
                return (LocalDate) columnValue;
            case TIMESTAMP:
                return ((LocalDateTime) columnValue).toLocalDate();
            case STRING:
                return LocalDate.parse((String) columnValue, DateTimeFormatter.BASIC_ISO_DATE);
            case INT:
                return LocalDate.ofEpochDay((int) columnValue);
            case BIGINT:
                return LocalDate.ofEpochDay((long) columnValue);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static LocalTime castAsTime(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case TIME:
                return (LocalTime) columnValue;
            case TIMESTAMP:
                return ((LocalDateTime) columnValue).toLocalTime();
            case INT:
                return LocalTime.ofSecondOfDay((int) columnValue);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static LocalDateTime castAsTimestamp(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case TIMESTAMP:
                return (LocalDateTime) columnValue;
            case DATE:
                return ((LocalDate) columnValue).atStartOfDay();
            case TIME:
                return ((LocalTime) columnValue).atDate(LocalDate.now());
            case INT:
                return Instant.ofEpochSecond((int) columnValue)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
            case BIGINT:
                return Instant.ofEpochMilli((long) columnValue)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
            case STRING:
                return LocalDateTime.parse((String) columnValue, BASIC_DATE_TIME);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Object[] castAsArray(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case ARRAY:
                return (Object[]) columnValue;
            case STRING:
                if (columnValue.toString().startsWith("[")
                        && columnValue.toString().endsWith("]")) {
                    return columnValue
                            .toString()
                            .substring(1, columnValue.toString().length() - 1)
                            .split(",");
                }
                return ((String) columnValue).split(",");
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) columnValue;
                String[] array = new String[row.getArity()];
                for (int i = 0; i < row.getArity(); i++) {
                    Object obj = row.getField(i);
                    if (obj != null) {
                        array[i] = obj.toString();
                    }
                }
                return array;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static Map castAsMap(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case MAP:
                return (Map) columnValue;
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) columnValue;
                SeaTunnelRowType rowType = (SeaTunnelRowType) inputColumn.getDataType();
                Map map = new LinkedHashMap<>();
                for (int i = 0; i < row.getArity(); i++) {
                    map.put(rowType.getFieldName(i), row.getField(i));
                }
                return map;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }

    public static SeaTunnelRow castAsRow(Column inputColumn, Object columnValue) {
        switch (inputColumn.getDataType().getSqlType()) {
            case NULL:
                return null;
            case ROW:
                return (SeaTunnelRow) columnValue;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + inputColumn.getDataType().getSqlType());
        }
    }
}
