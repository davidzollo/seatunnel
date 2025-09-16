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

package org.apache.seatunnel.connectors.seatunnel.pi.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PI Schema Builder
 *
 * <p>Supports fully user-customizable Schema with completely generic design names Supports
 * SeaTunnel standard schema format: schema = { fields { ... } }
 */
public class PISchemaBuilder {

    /**
     * Create SeaTunnelRowType based on user-configured schema, supporting new columns format and
     * legacy fields format
     */
    public static SeaTunnelRowType createRowTypeFromUserSchema(ReadonlyConfig config) {
        if (!config.getOptional(PIConfig.SCHEMA).isPresent()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID,
                    "Schema configuration not found, must define schema.columns or schema.fields");
        }

        try {
            // Read schema definition from user configuration - directly use Map type
            Map<String, Object> schemaMap = config.get(PIConfig.SCHEMA);

            // Prioritize new columns format
            if (schemaMap.containsKey("columns")) {
                return createRowTypeFromColumns(schemaMap);
            }
            // Backward compatibility: support legacy fields format
            else if (schemaMap.containsKey("fields")) {
                return createRowTypeFromFields(schemaMap);
            } else {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_INVALID,
                        "Schema configuration missing columns or fields definition");
            }

        } catch (PIConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID,
                    "Schema configuration parsing failed: " + e.getMessage(),
                    e);
        }
    }

    /** Create SeaTunnelRowType using new columns format */
    private static SeaTunnelRowType createRowTypeFromColumns(Map<String, Object> schemaMap) {
        Object columnsObj = schemaMap.get("columns");
        if (!(columnsObj instanceof List)) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "Schema columns must be an array");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columnsList = (List<Map<String, Object>>) columnsObj;

        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

        // Iterate through user-defined columns
        for (Map<String, Object> column : columnsList) {
            if (!column.containsKey("name") || !column.containsKey("type")) {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_INVALID,
                        "Each column must contain name and type fields");
            }

            String fieldName = column.get("name").toString();
            String fieldType = column.get("type").toString();

            fieldNames.add(fieldName);
            fieldTypes.add(parseSeaTunnelDataType(fieldType));
        }

        if (fieldNames.isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "No columns defined in Schema configuration");
        }

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    /** Create TableSchema with complete column information using new columns format */
    public static TableSchema createTableSchemaFromColumns(Map<String, Object> schemaMap) {
        Object columnsObj = schemaMap.get("columns");
        if (!(columnsObj instanceof List)) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "Schema columns must be an array");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columnsList = (List<Map<String, Object>>) columnsObj;

        List<Column> columns = new ArrayList<>();

        // Iterate through user-defined columns, creating complete Column objects
        for (Map<String, Object> columnConfig : columnsList) {
            if (!columnConfig.containsKey("name") || !columnConfig.containsKey("type")) {
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_INVALID,
                        "Each column must contain name and type fields");
            }

            String fieldName = columnConfig.get("name").toString();
            String fieldType = columnConfig.get("type").toString();

            // Parse columnLength
            Long columnLength = null;
            if (columnConfig.containsKey("columnLength")) {
                Object lengthObj = columnConfig.get("columnLength");
                if (lengthObj instanceof Number) {
                    columnLength = ((Number) lengthObj).longValue();
                }
            }

            // Parse nullable
            boolean nullable = true; // Default nullable
            if (columnConfig.containsKey("nullable")) {
                Object nullableObj = columnConfig.get("nullable");
                if (nullableObj instanceof Boolean) {
                    nullable = (Boolean) nullableObj;
                }
            }

            // Parse comment
            String comment = null;
            if (columnConfig.containsKey("comment")) {
                comment = columnConfig.get("comment").toString();
            }

            // Create Column object
            SeaTunnelDataType<?> dataType = parseSeaTunnelDataType(fieldType);
            Column column =
                    PhysicalColumn.of(
                            fieldName,
                            dataType,
                            columnLength,
                            nullable,
                            null, // defaultValue
                            comment);

            columns.add(column);
        }

        if (columns.isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "No columns defined in Schema configuration");
        }

        return TableSchema.builder().columns(columns).build();
    }

    /** Create SeaTunnelRowType using legacy fields format (backward compatibility) */
    private static SeaTunnelRowType createRowTypeFromFields(Map<String, Object> schemaMap) {
        Object fieldsObj = schemaMap.get("fields");
        if (!(fieldsObj instanceof Map)) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "Schema fields must be an object");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> fieldsMap = (Map<String, Object>) fieldsObj;

        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

        // Iterate through user-defined fields
        for (Map.Entry<String, Object> entry : fieldsMap.entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue().toString();

            fieldNames.add(fieldName);
            fieldTypes.add(parseSeaTunnelDataType(fieldType));
        }

        if (fieldNames.isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "No fields defined in Schema configuration");
        }

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    /**
     * Parse user-configured data type string to SeaTunnelDataType, supporting SeaTunnel standard
     * data types and all data types in customer business scenarios
     */
    private static SeaTunnelDataType<?> parseSeaTunnelDataType(String typeStr) {
        if (typeStr == null || typeStr.trim().isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "Data type string cannot be empty");
        }

        String cleanType = typeStr.trim().toLowerCase();

        switch (cleanType) {
            case "string":
                return BasicType.STRING_TYPE;
            case "int":
            case "integer":
                return BasicType.INT_TYPE;
            case "tinyint":
                return BasicType.BYTE_TYPE;
            case "smallint":
                return BasicType.SHORT_TYPE;
            case "bigint":
                return BasicType.LONG_TYPE;
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "bytes":
                return PrimitiveByteArrayType.INSTANCE;
            case "date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                return parseComplexDataType(typeStr.trim());
        }
    }

    /** Parse complex data types */
    private static SeaTunnelDataType<?> parseComplexDataType(String typeStr) {
        // Handle array type: array<elementType>
        if (typeStr.startsWith("array<") && typeStr.endsWith(">")) {
            String elementType = typeStr.substring(6, typeStr.length() - 1);
            SeaTunnelDataType<?> elementDataType = parseSeaTunnelDataType(elementType);
            switch (elementDataType.getSqlType()) {
                case STRING:
                    return ArrayType.STRING_ARRAY_TYPE;
                case BOOLEAN:
                    return ArrayType.BOOLEAN_ARRAY_TYPE;
                case TINYINT:
                    return ArrayType.BYTE_ARRAY_TYPE;
                case SMALLINT:
                    return ArrayType.SHORT_ARRAY_TYPE;
                case INT:
                    return ArrayType.INT_ARRAY_TYPE;
                case BIGINT:
                    return ArrayType.LONG_ARRAY_TYPE;
                case FLOAT:
                    return ArrayType.FLOAT_ARRAY_TYPE;
                case DOUBLE:
                    return ArrayType.DOUBLE_ARRAY_TYPE;
                default:
                    throw new PIConnectorException(
                            PIErrorCode.CONFIG_INVALID,
                            "Unsupported array element type: " + elementType);
            }
        }

        // Handle Map type: map<keyType, valueType>
        if (typeStr.startsWith("map<") && typeStr.endsWith(">")) {
            String mapContent = typeStr.substring(4, typeStr.length() - 1);
            String[] parts = splitMapTypes(mapContent);
            if (parts.length == 2) {
                SeaTunnelDataType<?> keyType = parseSeaTunnelDataType(parts[0].trim());
                SeaTunnelDataType<?> valueType = parseSeaTunnelDataType(parts[1].trim());
                return new MapType<>(keyType, valueType);
            } else {
                // Default to map<string, string>
                return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
            }
        }

        // Handle Decimal type: decimal(precision, scale)
        if (typeStr.startsWith("decimal(")) {
            try {
                String decimalContent = typeStr.substring(8, typeStr.length() - 1);
                String[] parts = decimalContent.split(",");
                if (parts.length == 2) {
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = Integer.parseInt(parts[1].trim());
                    return new DecimalType(precision, scale);
                } else {
                    // Default precision
                    return new DecimalType(38, 18);
                }
            } catch (Exception e) {
                // Parse failed, use default precision
                return new DecimalType(38, 18);
            }
        }

        throw new PIConnectorException(
                PIErrorCode.CONFIG_INVALID, "Unsupported data type: " + typeStr);
    }

    /**
     * Split Map type key-value types, handle nested generic types, such as map<string, array<int>>
     */
    private static String[] splitMapTypes(String mapContent) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketLevel = 0;

        for (int i = 0; i < mapContent.length(); i++) {
            char c = mapContent.charAt(i);

            if (c == '<') {
                bracketLevel++;
                current.append(c);
            } else if (c == '>') {
                bracketLevel--;
                current.append(c);
            } else if (c == ',' && bracketLevel == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts.toArray(new String[0]);
    }

    /**
     * Create default Schema (for cases without Schema configuration), includes all common fields,
     * supports all PI Web API data types
     */
    public static SeaTunnelRowType createDefaultRowType() {
        return new SeaTunnelRowType(
                new String[] {
                    "webid",
                    "name",
                    "path",
                    "timestamp",
                    "value_boolean",
                    "value_int",
                    "value_double",
                    "value_string",
                    "good",
                    "questionable",
                    "substituted",
                    "annotated",
                    "units_abbreviation",
                    "errors",
                    "data_type",
                    "source_system",
                    "created_at"
                },
                new SeaTunnelDataType[] {
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.DOUBLE_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.STRING_TYPE,
                    ArrayType.STRING_ARRAY_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE
                });
    }

    /** Validate Schema configuration reasonableness */
    public static void validateSchema(SeaTunnelRowType rowType) {
        if (rowType == null) {
            throw new PIConnectorException(PIErrorCode.CONFIG_INVALID, "Schema cannot be null");
        }

        if (rowType.getTotalFields() == 0) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_INVALID, "Schema must contain at least one field");
        }

        // Check for duplicate field names
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            for (int j = i + 1; j < fieldNames.length; j++) {
                if (fieldNames[i].equals(fieldNames[j])) {
                    throw new PIConnectorException(
                            PIErrorCode.CONFIG_INVALID,
                            "Duplicate field names found in Schema: " + fieldNames[i]);
                }
            }
        }
    }
}
