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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PI data type converter with fully generalized design (supports PI-CDC and PI), dynamically
 * converts PI data based on user-defined Schema and json_field configuration Supports all PI data
 * types: Digital, Int16, Float32, String, etc. No dependency on hardcoded model classes, completely
 * based on JSON dynamic parsing
 */
@Slf4j
public class PIDataTypeConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    /**
     * Convert directly from PI Web API JSON response to SeaTunnel row, supports user-defined
     * json_field mapping configuration
     */
    public static SeaTunnelRow convertFromJson(
            JsonNode itemNode,
            JsonNode dataPointNode,
            SeaTunnelRowType rowType,
            Map<String, String> jsonFieldMapping) {

        Object[] fields = new Object[rowType.getFieldNames().length];
        String[] fieldNames = rowType.getFieldNames();

        // Dynamically extract data based on user-defined field names and json_field mapping
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            SeaTunnelDataType fieldType = rowType.getFieldType(i);
            fields[i] =
                    extractFieldValueFromJson(
                            fieldName, fieldType, itemNode, dataPointNode, jsonFieldMapping);
        }

        return new SeaTunnelRow(fields);
    }

    /**
     * Parse PI Web API streaming response to SeaTunnel row list, completely based on JSON dynamic
     * parsing, no dependency on hardcoded models
     */
    public static List<SeaTunnelRow> parseBatchResponse(
            String responseJson, SeaTunnelRowType rowType, Map<String, String> jsonFieldMapping) {

        List<SeaTunnelRow> rows = new ArrayList<>();

        try {
            JsonNode rootNode = objectMapper.readTree(responseJson);
            JsonNode itemsNode = rootNode.path("Items");
            log.debug(
                    "Items node exists: {}, is array: {}, size: {}",
                    !itemsNode.isMissingNode(),
                    itemsNode.isArray(),
                    itemsNode.isArray() ? itemsNode.size() : 0);

            if (itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {

                    // Check if there are nested Items (data points)
                    JsonNode dataPointsNode = itemNode.path("Items");
                    if (dataPointsNode.isArray() && dataPointsNode.size() > 0) {
                        // Case with nested Items (e.g., batch query response)
                        log.debug(
                                "Found nested Items, data point count: {}", dataPointsNode.size());
                        for (JsonNode dataPointNode : dataPointsNode) {
                            SeaTunnelRow row =
                                    convertFromJson(
                                            itemNode, dataPointNode, rowType, jsonFieldMapping);
                            rows.add(row);
                        }
                    } else {
                        // Case without nested Items (e.g., single data point)
                        log.debug("No nested Items, directly process current node");
                        SeaTunnelRow row =
                                convertFromJson(itemNode, itemNode, rowType, jsonFieldMapping);
                        rows.add(row);
                    }
                }
            } else {
                log.warn(
                        "Items node is not an array or doesn't exist, response structure: {}",
                        rootNode.toString());
            }

            log.info("Parsing completed, generated row count: {}", rows.size());
        } catch (Exception e) {
            log.error("Failed to parse PI Web API response, response content: {}", responseJson, e);
            throw new PIConnectorException(
                    PIErrorCode.DATA_PARSE_FAILED,
                    "Failed to parse PI Web API response: " + e.getMessage(),
                    e);
        }

        return rows;
    }

    /**
     * Extract field value from JSON node, supports user-defined json_field mapping configuration
     */
    private static Object extractFieldValueFromJson(
            String fieldName,
            SeaTunnelDataType fieldType,
            JsonNode itemNode,
            JsonNode dataPointNode,
            Map<String, String> jsonFieldMapping) {

        // Get JSON path (supports user-defined mapping)
        String jsonPath = getJsonPath(fieldName, jsonFieldMapping);

        // Extract value based on JSON path
        Object rawValue = extractValueByJsonPath(jsonPath, itemNode, dataPointNode);

        // Convert value based on target type
        Object convertedValue = convertValueToTargetType(rawValue, fieldType);

        return convertedValue;
    }

    /** Get JSON path for the field */
    private static String getJsonPath(String fieldName, Map<String, String> jsonFieldMapping) {
        if (jsonFieldMapping != null && jsonFieldMapping.containsKey(fieldName)) {
            return jsonFieldMapping.get(fieldName);
        }

        // Default mapping rules
        switch (fieldName.toLowerCase()) {
            case "webid":
                return "$.WebId";
            case "name":
                return "$.Name";
            case "path":
                return "$.Path";
            case "timestamp":
                return "$.Timestamp";
            case "value":
                return "$.Value";
            case "good":
                return "$.Good";
            case "questionable":
                return "$.Questionable";
            case "substituted":
                return "$.Substituted";
            case "units":
            case "unitsabbreviation":
                return "$.UnitsAbbreviation";
            default:
                return "$." + fieldName;
        }
    }

    /** Extract value from node based on JSON path */
    private static Object extractValueByJsonPath(
            String jsonPath, JsonNode itemNode, JsonNode dataPointNode) {
        if (jsonPath == null || jsonPath.isEmpty()) {
            log.debug("JSON path is empty, return null");
            return null;
        }

        log.debug("Extract JSON path: {}", jsonPath);

        // Simplified JSONPath processing
        if (jsonPath.startsWith("$.")) {
            String fieldPath = jsonPath.substring(2);

            // First try to get from dataPointNode
            JsonNode value = getNestedValue(dataPointNode, fieldPath);
            if (value != null && !value.isMissingNode()) {
                Object parsedValue = parseJsonValue(value);
                return parsedValue;
            }

            // Then try to get from itemNode
            value = getNestedValue(itemNode, fieldPath);
            if (value != null && !value.isMissingNode()) {
                Object parsedValue = parseJsonValue(value);
                return parsedValue;
            }

            // Smart field extraction: support multiple path formats
            return extractSmartField(fieldPath, dataPointNode, itemNode);
        }

        log.info("No matching value found, return null");
        return null;
    }

    /** Smart field extraction: support multiple path formats and field types */
    private static Object extractSmartField(
            String fieldPath, JsonNode dataPointNode, JsonNode itemNode) {
        // Extract field name (remove path prefix)
        String fieldName = extractFieldName(fieldPath);

        // First try to get field directly from dataPointNode
        if (dataPointNode != null && dataPointNode.has(fieldName)) {
            JsonNode fieldNode = dataPointNode.get(fieldName);
            Object value = parseJsonValue(fieldNode);
            return value;
        }

        // Then try to get field directly from itemNode
        if (itemNode != null && itemNode.has(fieldName)) {
            JsonNode fieldNode = itemNode.get(fieldName);
            Object value = parseJsonValue(fieldNode);
            return value;
        }

        // Special handling for Value field: support nested structure
        if ("Value".equals(fieldName)) {
            return extractSmartValue(dataPointNode, itemNode);
        }

        log.info("Field extraction failed, field {} not found", fieldName);
        return null;
    }

    /** Extract field name from field path */
    private static String extractFieldName(String fieldPath) {
        if (fieldPath == null) return null;

        // Process $.FieldName format
        if (fieldPath.startsWith("$.")) {
            return fieldPath.substring(2);
        }

        // Process $.Items[*].FieldName format
        if (fieldPath.contains("Items[*].")) {
            return fieldPath.substring(fieldPath.lastIndexOf(".") + 1);
        }

        // Process other formats, take the content after the last dot
        if (fieldPath.contains(".")) {
            return fieldPath.substring(fieldPath.lastIndexOf(".") + 1);
        }

        return fieldPath;
    }

    /** Smart extraction of Value field value */
    private static Object extractSmartValue(JsonNode dataPointNode, JsonNode itemNode) {
        // First try to get from Value field of dataPointNode
        if (dataPointNode != null && dataPointNode.has("Value")) {
            JsonNode valueNode = dataPointNode.get("Value");

            // If Value is an object, try to get the Value field
            if (valueNode.isObject() && valueNode.has("Value")) {
                return parseJsonValue(valueNode.get("Value"));
            }

            // If Value is a direct number
            if (valueNode.isNumber() || valueNode.isTextual()) {
                return parseJsonValue(valueNode);
            }
        }

        // Then try to get from Value field of itemNode
        if (itemNode != null && itemNode.has("Value")) {
            JsonNode valueNode = itemNode.get("Value");

            // If Value is an object, try to get the Value field
            if (valueNode.isObject() && valueNode.has("Value")) {
                return parseJsonValue(valueNode.get("Value"));
            }

            // If Value is a direct number
            if (valueNode.isNumber() || valueNode.isTextual()) {
                return parseJsonValue(valueNode);
            }
        }

        return null;
    }

    /** Get nested JSON value */
    private static JsonNode getNestedValue(JsonNode node, String fieldPath) {
        if (node == null || fieldPath == null) {
            return null;
        }

        String[] parts = fieldPath.split("\\.");
        JsonNode current = node;

        for (String part : parts) {
            if (current == null || current.isMissingNode()) {
                return null;
            }

            // Process array index syntax, such as Items[*] or Items[0]
            if (part.contains("[") && part.contains("]")) {
                String arrayName = part.substring(0, part.indexOf("["));
                String indexPart = part.substring(part.indexOf("[") + 1, part.indexOf("]"));

                // Get array node
                JsonNode arrayNode = current.path(arrayName);
                if (!arrayNode.isArray()) {
                    return null;
                }

                // Process array index
                if ("*".equals(indexPart)) {
                    // For [*] syntax, return the first element
                    // Note: array traversal has already been processed in the parseStreamResponse
                    // method above
                    if (arrayNode.size() > 0) {
                        current = arrayNode.get(0);
                    } else {
                        return null;
                    }
                } else {
                    // Process specific index, such as [0]
                    try {
                        int index = Integer.parseInt(indexPart);
                        if (index >= 0 && index < arrayNode.size()) {
                            current = arrayNode.get(index);
                        } else {
                            return null;
                        }
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
            } else {
                // Normal field access
                current = current.path(part);
            }
        }

        return current;
    }

    /** Parse JSON value to Java object */
    private static Object parseJsonValue(JsonNode valueNode) {
        if (valueNode.isNull()) {
            return null;
        }

        // Process Digital type (object form)
        if (valueNode.isObject() && valueNode.has("Name") && valueNode.has("Value")) {
            String name = valueNode.path("Name").asText();
            int value = valueNode.path("Value").asInt();

            // Return boolean or original string
            if ("ON".equalsIgnoreCase(name) || "OFF".equalsIgnoreCase(name)) {
                return "ON".equalsIgnoreCase(name);
            }

            return name;
        }

        // Process basic types
        if (valueNode.isBoolean()) {
            return valueNode.asBoolean();
        } else if (valueNode.isInt()) {
            return valueNode.asInt();
        } else if (valueNode.isLong()) {
            return valueNode.asLong();
        } else if (valueNode.isDouble()) {
            return valueNode.asDouble();
        } else if (valueNode.isTextual()) {
            return valueNode.asText();
        }

        // Default return string
        return valueNode.asText();
    }

    /** Convert raw value to target type */
    private static Object convertValueToTargetType(Object rawValue, SeaTunnelDataType targetType) {
        if (rawValue == null) {
            return null;
        }

        try {
            // String type
            if (targetType.equals(BasicType.STRING_TYPE)) {
                return rawValue.toString();
            }

            // Boolean type
            if (targetType.equals(BasicType.BOOLEAN_TYPE)) {
                return convertToBoolean(rawValue);
            }

            // Integer type
            if (targetType.equals(BasicType.INT_TYPE)) {
                return convertToInteger(rawValue);
            }

            // Long type
            if (targetType.equals(BasicType.LONG_TYPE)) {
                return convertToLong(rawValue);
            }

            // Float type
            if (targetType.equals(BasicType.FLOAT_TYPE)) {
                return convertToFloat(rawValue);
            }

            // Double type
            if (targetType.equals(BasicType.DOUBLE_TYPE)) {
                return convertToDouble(rawValue);
            }

            // Timestamp type
            if (targetType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
                return convertToLocalDateTime(rawValue);
            }

            // Default return string
            return rawValue.toString();

        } catch (Exception e) {
            log.warn(
                    "Type conversion failed, field value: {}, target type: {}, error: {}",
                    rawValue,
                    targetType,
                    e.getMessage());
            return null;
        }
    }

    /** Convert to boolean */
    private static Boolean convertToBoolean(Object value) {
        if (value == null) return null;

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }

        String str = value.toString().trim().toLowerCase();
        return "true".equals(str) || "1".equals(str) || "on".equals(str) || "yes".equals(str);
    }

    /** Convert to integer */
    private static Integer convertToInteger(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Convert to long */
    private static Long convertToLong(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Convert to float */
    private static Float convertToFloat(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        try {
            return Float.parseFloat(value.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Convert to double */
    private static Double convertToDouble(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        try {
            return Double.parseDouble(value.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Convert to local date time */
    private static LocalDateTime convertToLocalDateTime(Object value) {
        if (value == null) return null;

        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }

        String timestampStr = value.toString().trim();
        if (timestampStr.isEmpty()) {
            return null;
        }

        try {
            // Try to parse ISO format
            ZonedDateTime zdt = ZonedDateTime.parse(timestampStr, ISO_FORMATTER.withZone(UTC_ZONE));
            return zdt.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
        } catch (DateTimeParseException e) {
            try {
                // Try to parse without timezone
                return LocalDateTime.parse(timestampStr, ISO_FORMATTER);
            } catch (DateTimeParseException e2) {
                try {
                    // Try to parse more relaxed format
                    return LocalDateTime.parse(
                            timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                } catch (DateTimeParseException e3) {
                    log.warn("Failed to parse timestamp: {}", timestampStr);
                    return null;
                }
            }
        }
    }
}
