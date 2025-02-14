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

package org.apache.seatunnel.format.csv;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.EncodingUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.csv.constant.CsvFormatConstant;
import org.apache.seatunnel.format.csv.exception.SeaTunnelCsvFormatException;
import org.apache.seatunnel.format.csv.processor.CsvLineProcessor;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class CsvDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final DateUtils.Formatter dateFormatter;
    private final DateTimeUtils.Formatter dateTimeFormatter;
    private final TimeUtils.Formatter timeFormatter;
    private final String encoding;
    private final String nullFormat;
    private final CatalogTable catalogTable;
    private final CsvLineProcessor processor;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public Map<String, DateTimeFormatter> fieldFormatterMap = new HashMap<>();

    private CsvDeserializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            DateUtils.Formatter dateFormatter,
            DateTimeUtils.Formatter dateTimeFormatter,
            TimeUtils.Formatter timeFormatter,
            String encoding,
            String nullFormat,
            CatalogTable catalogTable,
            CsvLineProcessor processor) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.separators = separators;
        this.dateFormatter = dateFormatter;
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeFormatter = timeFormatter;
        this.encoding = encoding;
        this.catalogTable = catalogTable;
        this.nullFormat = nullFormat;
        this.processor = processor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private CatalogTable catalogTable;
        private String[] separators = CsvFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private String encoding = StandardCharsets.UTF_8.name();
        private String nullFormat;
        private CsvLineProcessor csvLineProcessor = new DefaultCsvLineProcessor();

        private Builder() {}

        public Builder setCatalogTable(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
            return this;
        }

        public Builder seaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
            this.seaTunnelRowType = seaTunnelRowType;
            return this;
        }

        public Builder delimiter(String delimiter) {
            this.separators[0] = delimiter;
            return this;
        }

        public Builder separators(String[] separators) {
            this.separators = separators;
            return this;
        }

        public Builder dateFormatter(DateUtils.Formatter dateFormatter) {
            this.dateFormatter = dateFormatter;
            return this;
        }

        public Builder dateTimeFormatter(DateTimeUtils.Formatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        public Builder timeFormatter(TimeUtils.Formatter timeFormatter) {
            this.timeFormatter = timeFormatter;
            return this;
        }

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder nullFormat(String nullFormat) {
            this.nullFormat = nullFormat;
            return this;
        }

        public Builder csvLineProcessor(CsvLineProcessor csvLineProcessor) {
            this.csvLineProcessor = csvLineProcessor;
            return this;
        }

        public CsvDeserializationSchema build() {
            return new CsvDeserializationSchema(
                    seaTunnelRowType,
                    separators,
                    dateFormatter,
                    dateTimeFormatter,
                    timeFormatter,
                    encoding,
                    nullFormat,
                    catalogTable,
                    csvLineProcessor);
        }
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType, 0);
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            String fieldValue = splitsMap.get(i);
            if (StringUtils.isBlank(fieldValue)) {
                continue;
            }
            if (StringUtils.equals(fieldValue, nullFormat)) {
                continue;
            }
            objects[i] =
                    convert(
                            fieldValue,
                            seaTunnelRowType.getFieldType(i),
                            0,
                            seaTunnelRowType.getFieldNames()[i]);
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(objects);
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            seaTunnelRow.setTableId(tablePath.toString());
        }
        return seaTunnelRow;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    Map<Integer, String> splitLineBySeaTunnelRowType(
            String line, SeaTunnelRowType seaTunnelRowType, int level) {
        String[] splits = processor.splitLine(line, separators[level]);
        LinkedHashMap<Integer, String> splitsMap = new LinkedHashMap<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();

        for (int i = 0; i < splits.length; i++) {
            splitsMap.put(i, splits[i]);
        }

        if (fieldTypes.length > splits.length) {
            // contains partition columns
            for (int i = splits.length; i < fieldTypes.length; i++) {
                splitsMap.put(i, null);
            }
        }
        return splitsMap;
    }

    private Object convert(
            String field, SeaTunnelDataType<?> fieldType, int level, String fieldName) {
        if (StringUtils.isBlank(field)) {
            return null;
        }

        try {
            switch (fieldType.getSqlType()) {
                case ARRAY:
                    SeaTunnelDataType<?> elementType =
                            ((ArrayType<?, ?>) fieldType).getElementType();
                    String[] elements = field.split(separators[level + 1]);
                    List<Object> objectList = new ArrayList<>();
                    for (String element : elements) {
                        objectList.add(convert(element, elementType, level + 1, fieldName));
                    }
                    return convertListToArray(objectList, elementType);

                case MAP:
                    SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                    SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                    LinkedHashMap<Object, Object> objectMap = new LinkedHashMap<>();
                    String[] kvs = field.split(separators[level + 1]);
                    for (String kv : kvs) {
                        String[] splits = kv.split(separators[level + 2]);
                        if (splits.length < 2) {
                            throw new SeaTunnelCsvFormatException(
                                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                    "Invalid map format: " + kv);
                        }
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName),
                                convert(splits[1], valueType, level + 1, fieldName));
                    }
                    return objectMap;

                case STRING:
                    return field;

                case BOOLEAN:
                    return Boolean.parseBoolean(field);

                case TINYINT:
                    return Byte.parseByte(field);

                case SMALLINT:
                    return Short.parseShort(field);

                case INT:
                    return Integer.parseInt(field);

                case BIGINT:
                    return Long.parseLong(field);

                case FLOAT:
                    return Float.parseFloat(field);

                case DOUBLE:
                    return Double.parseDouble(field);

                case DECIMAL:
                    return new BigDecimal(field);

                case NULL:
                    return null;

                case BYTES:
                    return field.getBytes(StandardCharsets.UTF_8);

                case DATE:
                    return parseDate(field, fieldName);

                case TIME:
                    return parseTime(field);

                case TIMESTAMP:
                    return parseTimestamp(field, fieldName);

                case ROW:
                    Map<Integer, String> splitsMap =
                            splitLineBySeaTunnelRowType(
                                    field, (SeaTunnelRowType) fieldType, level + 1);
                    Object[] objects = new Object[splitsMap.size()];
                    String[] eleFieldNames = ((SeaTunnelRowType) fieldType).getFieldNames();
                    for (int i = 0; i < objects.length; i++) {
                        objects[i] =
                                convert(
                                        splitsMap.get(i),
                                        ((SeaTunnelRowType) fieldType).getFieldType(i),
                                        level + 1,
                                        fieldName + "." + eleFieldNames[i]);
                    }
                    return new SeaTunnelRow(objects);

                default:
                    throw new SeaTunnelCsvFormatException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            String.format(
                                    "SeaTunnel not support this data type [%s]",
                                    fieldType.getSqlType()));
            }
        } catch (Exception e) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, e.getMessage(), e);
        }
    }

    private Object[] convertListToArray(List<Object> list, SeaTunnelDataType<?> elementType) {
        switch (elementType.getSqlType()) {
            case STRING:
                return list.toArray(new String[0]);
            case BOOLEAN:
                return list.toArray(new Boolean[0]);
            case TINYINT:
                return list.toArray(new Byte[0]);
            case SMALLINT:
                return list.toArray(new Short[0]);
            case INT:
                return list.toArray(new Integer[0]);
            case BIGINT:
                return list.toArray(new Long[0]);
            case FLOAT:
                return list.toArray(new Float[0]);
            case DOUBLE:
                return list.toArray(new Double[0]);
            case DECIMAL:
                return list.toArray(new BigDecimal[0]);
            case DATE:
                return list.toArray(new LocalDate[0]);
            case TIME:
                return list.toArray(new LocalTime[0]);
            case TIMESTAMP:
                return list.toArray(new LocalDateTime[0]);
            default:
                throw new SeaTunnelCsvFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel array not support this data type [%s]",
                                elementType.getSqlType()));
        }
    }

    private LocalDate parseDate(String field, String fieldName) {
        DateTimeFormatter dateFormatter =
                fieldFormatterMap.computeIfAbsent(
                        fieldName, f -> DateUtils.matchDateTimeFormatter(field));
        if (dateFormatter == null) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "SeaTunnel can not parse this date format [%s] of field [%s]",
                            field, fieldName));
        }
        return dateFormatter.parse(field, TemporalQueries.localDate());
    }

    private LocalTime parseTime(String field) {
        try {
            TemporalAccessor parsedTime = TIME_FORMAT.parse(field);
            return parsedTime.query(TemporalQueries.localTime());
        } catch (DateTimeParseException e) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Invalid time format: " + field, e);
        }
    }

    private LocalDateTime parseTimestamp(String field, String fieldName) {
        DateTimeFormatter dateTimeFormatter =
                fieldFormatterMap.computeIfAbsent(
                        fieldName, f -> DateTimeUtils.matchDateTimeFormatter(field));
        if (dateTimeFormatter == null) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "SeaTunnel can not parse this date format [%s] of field [%s]",
                            field, fieldName));
        }
        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(field);
        return LocalDateTime.of(
                parsedTimestamp.query(TemporalQueries.localDate()),
                parsedTimestamp.query(TemporalQueries.localTime()));
    }
}
