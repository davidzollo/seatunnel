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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PIDataTypeConverterTest {

    private ObjectMapper objectMapper;
    private SeaTunnelRowType basicRowType;
    private SeaTunnelRowType complexRowType;

    @BeforeEach
    public void setUp() {
        objectMapper = new ObjectMapper();

        basicRowType =
                new SeaTunnelRowType(
                        new String[] {"webid", "name", "value", "timestamp"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.DOUBLE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        complexRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "webid",
                            "name",
                            "path",
                            "timestamp",
                            "value",
                            "good",
                            "questionable",
                            "substituted",
                            "units"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.STRING_TYPE
                        });
    }

    @Test
    public void testConvertFromJsonWithAllStandardFields() throws Exception {
        String itemJson =
                "{\n"
                        + "    \"WebId\": \"test-webid-123\",\n"
                        + "    \"Name\": \"Temperature.Sensor1\",\n"
                        + "    \"Path\": \"\\\\\\\\PIServer\\\\Database\\\\Element\"\n"
                        + "}";

        String dataPointJson =
                "{\n"
                        + "    \"Timestamp\": \"2023-12-01T10:30:00.123Z\",\n"
                        + "    \"Value\": 123.45,\n"
                        + "    \"Good\": true,\n"
                        + "    \"Questionable\": false,\n"
                        + "    \"Substituted\": false,\n"
                        + "    \"UnitsAbbreviation\": \"°C\"\n"
                        + "}";

        JsonNode itemNode = objectMapper.readTree(itemJson);
        JsonNode dataPointNode = objectMapper.readTree(dataPointJson);

        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(itemNode, dataPointNode, complexRowType, null);

        Assertions.assertEquals("test-webid-123", row.getField(0));
        Assertions.assertEquals("Temperature.Sensor1", row.getField(1));
        Assertions.assertEquals("\\\\PIServer\\Database\\Element", row.getField(2));
        Assertions.assertNotNull(row.getField(3));
        Assertions.assertTrue(row.getField(3) instanceof LocalDateTime);
        Assertions.assertEquals(123.45, row.getField(4));
        Assertions.assertEquals(true, row.getField(5));
        Assertions.assertEquals(false, row.getField(6));
        Assertions.assertEquals(false, row.getField(7));
        Assertions.assertEquals("°C", row.getField(8));
    }

    @Test
    public void testConvertFromJsonWithCustomFieldMapping() throws Exception {
        String itemJson =
                "{\n"
                        + "    \"CustomWebId\": \"custom-webid-456\",\n"
                        + "    \"CustomName\": \"Pressure.Sensor2\",\n"
                        + "    \"CustomPath\": \"\\\\\\\\CustomServer\\\\DB\\\\Tag\"\n"
                        + "}";

        String dataPointJson =
                "{\n"
                        + "    \"CustomTimestamp\": \"2023-12-02T15:45:30.456Z\",\n"
                        + "    \"CustomValue\": 789.12,\n"
                        + "    \"CustomGood\": false,\n"
                        + "    \"CustomUnits\": \"bar\"\n"
                        + "}";

        JsonNode itemNode = objectMapper.readTree(itemJson);
        JsonNode dataPointNode = objectMapper.readTree(dataPointJson);

        Map<String, String> jsonFieldMapping = new HashMap<>();
        jsonFieldMapping.put("webid", "$.CustomWebId");
        jsonFieldMapping.put("name", "$.CustomName");
        jsonFieldMapping.put("path", "$.CustomPath");
        jsonFieldMapping.put("timestamp", "$.CustomTimestamp");
        jsonFieldMapping.put("value", "$.CustomValue");
        jsonFieldMapping.put("good", "$.CustomGood");
        jsonFieldMapping.put("units", "$.CustomUnits");

        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(
                        itemNode, dataPointNode, complexRowType, jsonFieldMapping);

        Assertions.assertEquals("custom-webid-456", row.getField(0));
        Assertions.assertEquals("Pressure.Sensor2", row.getField(1));
        Assertions.assertEquals("\\\\CustomServer\\DB\\Tag", row.getField(2));
        Assertions.assertNotNull(row.getField(3));
        Assertions.assertEquals(789.12, row.getField(4));
        Assertions.assertEquals(false, row.getField(5));
        Assertions.assertEquals("bar", row.getField(8));
    }

    @Test
    public void testConvertFromJsonWithNullAndMissingValues() throws Exception {
        String itemJson =
                "{\n" + "    \"WebId\": \"test-webid-null\",\n" + "    \"Name\": null\n" + "}";

        String dataPointJson =
                "{\n"
                        + "    \"Timestamp\": \"2023-12-01T10:30:00Z\",\n"
                        + "    \"Value\": null,\n"
                        + "    \"Good\": true\n"
                        + "}";

        JsonNode itemNode = objectMapper.readTree(itemJson);
        JsonNode dataPointNode = objectMapper.readTree(dataPointJson);

        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(itemNode, dataPointNode, basicRowType, null);

        Assertions.assertEquals("test-webid-null", row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertNull(row.getField(2));
        Assertions.assertNotNull(row.getField(3));
    }

    @Test
    public void testConvertFromJsonWithDifferentDataTypes() throws Exception {
        String dataJson =
                "{\n"
                        + "    \"StringValue\": \"test-string\",\n"
                        + "    \"IntValue\": 42,\n"
                        + "    \"LongValue\": 9223372036854775807,\n"
                        + "    \"FloatValue\": 3.14,\n"
                        + "    \"DoubleValue\": 2.718281828459045,\n"
                        + "    \"BooleanValue\": true,\n"
                        + "    \"TimestampValue\": \"2023-12-01T10:30:00.123Z\"\n"
                        + "}";

        JsonNode dataNode = objectMapper.readTree(dataJson);

        SeaTunnelRowType typeTestRowType =
                new SeaTunnelRowType(
                        new String[] {"str", "int", "long", "float", "double", "bool", "timestamp"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        Map<String, String> mapping = new HashMap<>();
        mapping.put("str", "$.StringValue");
        mapping.put("int", "$.IntValue");
        mapping.put("long", "$.LongValue");
        mapping.put("float", "$.FloatValue");
        mapping.put("double", "$.DoubleValue");
        mapping.put("bool", "$.BooleanValue");
        mapping.put("timestamp", "$.TimestampValue");

        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(dataNode, dataNode, typeTestRowType, mapping);

        Assertions.assertEquals("test-string", row.getField(0));
        Assertions.assertEquals(42, row.getField(1));
        Assertions.assertEquals(9223372036854775807L, row.getField(2));
        Assertions.assertEquals(3.14f, ((Number) row.getField(3)).floatValue(), 0.001f);
        Assertions.assertEquals(
                2.718281828459045, ((Number) row.getField(4)).doubleValue(), 0.000000000000001);
        Assertions.assertEquals(true, row.getField(5));
        Assertions.assertInstanceOf(LocalDateTime.class, row.getField(6));
    }

    @Test
    public void testConvertFromJsonWithInvalidTimestamp() throws Exception {
        String dataJson =
                "{\n"
                        + "    \"WebId\": \"test-webid\",\n"
                        + "    \"Timestamp\": \"invalid-timestamp\"\n"
                        + "}";

        JsonNode dataNode = objectMapper.readTree(dataJson);
        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(dataNode, dataNode, basicRowType, null);

        Assertions.assertEquals("test-webid", row.getField(0));
        // Invalid timestamp will return null
        Assertions.assertNull(row.getField(3));
    }

    @Test
    public void testParseBatchResponseWithNestedStructure() throws Exception {
        String responseJson =
                "{\n"
                        + "    \"Items\": [\n"
                        + "        {\n"
                        + "            \"WebId\": \"point1-webid\",\n"
                        + "            \"Name\": \"Temperature.Zone1\",\n"
                        + "            \"Path\": \"\\\\\\\\Server\\\\DB\\\\Zone1.Temp\",\n"
                        + "            \"Items\": [\n"
                        + "                {\n"
                        + "                    \"Timestamp\": \"2023-12-01T10:00:00.000Z\",\n"
                        + "                    \"Value\": 25.5,\n"
                        + "                    \"Good\": true,\n"
                        + "                    \"Questionable\": false,\n"
                        + "                    \"Substituted\": false,\n"
                        + "                    \"UnitsAbbreviation\": \"°C\"\n"
                        + "                },\n"
                        + "                {\n"
                        + "                    \"Timestamp\": \"2023-12-01T11:00:00.000Z\",\n"
                        + "                    \"Value\": 26.2,\n"
                        + "                    \"Good\": true,\n"
                        + "                    \"Questionable\": false,\n"
                        + "                    \"Substituted\": false,\n"
                        + "                    \"UnitsAbbreviation\": \"°C\"\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        },\n"
                        + "        {\n"
                        + "            \"WebId\": \"point2-webid\",\n"
                        + "            \"Name\": \"Pressure.Zone1\",\n"
                        + "            \"Path\": \"\\\\\\\\Server\\\\DB\\\\Zone1.Press\",\n"
                        + "            \"Items\": [\n"
                        + "                {\n"
                        + "                    \"Timestamp\": \"2023-12-01T10:00:00.000Z\",\n"
                        + "                    \"Value\": 1.013,\n"
                        + "                    \"Good\": false,\n"
                        + "                    \"Questionable\": true,\n"
                        + "                    \"Substituted\": false,\n"
                        + "                    \"UnitsAbbreviation\": \"bar\"\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";

        List<SeaTunnelRow> rows =
                PIDataTypeConverter.parseBatchResponse(responseJson, complexRowType, null);

        Assertions.assertEquals(3, rows.size());

        SeaTunnelRow row1 = rows.get(0);
        Assertions.assertEquals("point1-webid", row1.getField(0));
        Assertions.assertEquals("Temperature.Zone1", row1.getField(1));
        Assertions.assertEquals("\\\\Server\\DB\\Zone1.Temp", row1.getField(2));
        Assertions.assertEquals(25.5, row1.getField(4));
        Assertions.assertEquals(true, row1.getField(5));
        Assertions.assertEquals(false, row1.getField(6));
        Assertions.assertEquals(false, row1.getField(7));
        Assertions.assertEquals("°C", row1.getField(8));

        SeaTunnelRow row2 = rows.get(1);
        Assertions.assertEquals("point1-webid", row2.getField(0));
        Assertions.assertEquals(26.2, row2.getField(4));
        Assertions.assertEquals(true, row2.getField(5));

        SeaTunnelRow row3 = rows.get(2);
        Assertions.assertEquals("point2-webid", row3.getField(0));
        Assertions.assertEquals("Pressure.Zone1", row3.getField(1));
        Assertions.assertEquals(1.013, row3.getField(4));
        Assertions.assertEquals(false, row3.getField(5));
        Assertions.assertEquals(true, row3.getField(6));
        Assertions.assertEquals("bar", row3.getField(8));
    }

    @Test
    public void testParseBatchResponseWithEmptyItems() throws Exception {
        String responseJson = "{\n" + "    \"Items\": []\n" + "}";

        List<SeaTunnelRow> rows =
                PIDataTypeConverter.parseBatchResponse(responseJson, basicRowType, null);

        Assertions.assertNotNull(rows);
        Assertions.assertEquals(0, rows.size());
    }

    @Test
    public void testParseBatchResponseWithNoDataPoints() throws Exception {
        String responseJson =
                "{\n"
                        + "    \"Items\": [\n"
                        + "        {\n"
                        + "            \"WebId\": \"point1-webid\",\n"
                        + "            \"Name\": \"EmptyPoint\",\n"
                        + "            \"Items\": []\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";

        List<SeaTunnelRow> rows =
                PIDataTypeConverter.parseBatchResponse(responseJson, basicRowType, null);

        Assertions.assertNotNull(rows);
        // Actual implementation: even without data points, creates one row record for each Item
        Assertions.assertEquals(1, rows.size());

        // Verify that returned row contains Item information but data point fields are null
        SeaTunnelRow row = rows.get(0);
        Assertions.assertEquals("point1-webid", row.getField(0));
        Assertions.assertEquals("EmptyPoint", row.getField(1));
        // value and timestamp fields should be null because there are no data points
        Assertions.assertNull(row.getField(2)); // value
        Assertions.assertNull(row.getField(3)); // timestamp
    }

    @Test
    public void testParseBatchResponseWithInvalidJson() {
        String invalidJson = "{ invalid json structure }";

        Assertions.assertThrows(
                PIConnectorException.class,
                () -> {
                    PIDataTypeConverter.parseBatchResponse(invalidJson, basicRowType, null);
                });
    }

    @Test
    public void testParseBatchResponseWithMissingItemsField() throws Exception {
        String responseJson = "{\n" + "    \"Data\": []\n" + "}";

        List<SeaTunnelRow> rows =
                PIDataTypeConverter.parseBatchResponse(responseJson, basicRowType, null);
        Assertions.assertNotNull(rows);
        Assertions.assertEquals(0, rows.size());
    }

    @Test
    public void testParseBatchResponseWithCustomMapping() throws Exception {
        String responseJson =
                "{\n"
                        + "    \"Items\": [\n"
                        + "        {\n"
                        + "            \"CustomWebId\": \"custom-point1\",\n"
                        + "            \"CustomName\": \"CustomTemp\",\n"
                        + "            \"Items\": [\n"
                        + "                {\n"
                        + "                    \"CustomTimestamp\": \"2023-12-01T10:00:00Z\",\n"
                        + "                    \"CustomValue\": 30.5\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";

        Map<String, String> customMapping = new HashMap<>();
        customMapping.put("webid", "$.CustomWebId");
        customMapping.put("name", "$.CustomName");
        customMapping.put("timestamp", "$.CustomTimestamp");
        customMapping.put("value", "$.CustomValue");

        List<SeaTunnelRow> rows =
                PIDataTypeConverter.parseBatchResponse(responseJson, basicRowType, customMapping);

        Assertions.assertEquals(1, rows.size());
        SeaTunnelRow row = rows.get(0);
        Assertions.assertEquals("custom-point1", row.getField(0));
        Assertions.assertEquals("CustomTemp", row.getField(1));
        Assertions.assertEquals(30.5, row.getField(2));
        Assertions.assertNotNull(row.getField(3));
    }

    @Test
    public void testTimestampParsing() throws Exception {
        String[] timestampFormats = {
            "2023-12-01T10:30:00Z",
            "2023-12-01T10:30:00.123Z",
            "2023-12-01T10:30:00.123456Z",
            "2023-12-01T10:30:00+08:00",
            "2023-12-01T10:30:00.123+08:00"
        };

        for (String timestamp : timestampFormats) {
            String dataJson =
                    "{\n"
                            + "    \"WebId\": \"test-webid\",\n"
                            + "    \"Timestamp\": \""
                            + timestamp
                            + "\"\n"
                            + "}";

            JsonNode dataNode = objectMapper.readTree(dataJson);
            SeaTunnelRow row =
                    PIDataTypeConverter.convertFromJson(dataNode, dataNode, basicRowType, null);

            Assertions.assertNotNull(row.getField(3), "Failed to parse timestamp: " + timestamp);
            Assertions.assertInstanceOf(LocalDateTime.class, row.getField(3));
        }
    }

    @Test
    public void testNumericValueConversions() throws Exception {
        Object[] testValues = {
            42, // int
            42L, // long
            42.0f, // float
            42.0, // double
            "42", // string number
            "42.5" // string decimal
        };

        for (Object value : testValues) {
            String dataJson =
                    "{\n"
                            + "    \"WebId\": \"test-webid\",\n"
                            + "    \"Value\": "
                            + (value instanceof String ? "\"" + value + "\"" : value)
                            + "\n"
                            + "}";

            JsonNode dataNode = objectMapper.readTree(dataJson);
            SeaTunnelRow row =
                    PIDataTypeConverter.convertFromJson(dataNode, dataNode, basicRowType, null);

            Assertions.assertNotNull(row.getField(2), "Failed to convert value: " + value);
            Assertions.assertInstanceOf(Number.class, row.getField(2));
        }
    }

    @Test
    public void testBooleanValueConversions() throws Exception {
        Object[] testValues = {true, false, "true", "false", "TRUE", "FALSE", 1, 0};

        SeaTunnelRowType boolRowType =
                new SeaTunnelRowType(
                        new String[] {"webid", "bool_value"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE});

        for (Object value : testValues) {
            String dataJson =
                    "{\n"
                            + "    \"WebId\": \"test-webid\",\n"
                            + "    \"BoolValue\": "
                            + (value instanceof String ? "\"" + value + "\"" : value)
                            + "\n"
                            + "}";

            JsonNode dataNode = objectMapper.readTree(dataJson);

            Map<String, String> mapping = new HashMap<>();
            mapping.put("webid", "$.WebId");
            mapping.put("bool_value", "$.BoolValue");

            SeaTunnelRow row =
                    PIDataTypeConverter.convertFromJson(dataNode, dataNode, boolRowType, mapping);

            Assertions.assertNotNull(row.getField(1), "Failed to convert boolean value: " + value);
            Assertions.assertInstanceOf(Boolean.class, row.getField(1));
        }
    }

    @Test
    public void testErrorHandlingForUnsupportedDataType() throws Exception {
        SeaTunnelRowType unsupportedRowType =
                new SeaTunnelRowType(
                        new String[] {"webid", "unsupported"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, ArrayType.BYTE_ARRAY_TYPE});

        String dataJson =
                "{\n"
                        + "    \"WebId\": \"test-webid\",\n"
                        + "    \"UnsupportedField\": \"some-value\"\n"
                        + "}";

        JsonNode dataNode = objectMapper.readTree(dataJson);

        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(dataNode, dataNode, unsupportedRowType, null);

        Assertions.assertNotNull(row);
        Assertions.assertEquals("test-webid", row.getField(0));
    }
}
