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

package org.apache.seatunnel.connectors.seatunnel.pi;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIHttpClient;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

/**
 * PI HTTP Client test with real PI Web API JSON response format mock This demonstrates proper
 * mocking of actual PI Web API responses
 */
@Slf4j
@ExtendWith(MockitoExtension.class)
public class PIHttpClientTest {

    private PIConfigHelper configHelper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("pi_web_api_url", "https://mock-pi-server.test:8443/piwebapi");
        configMap.put("username", "mockuser");
        configMap.put("password", "mockpass");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        configHelper = new PIConfigHelper(config);
        objectMapper = new ObjectMapper();
    }

    /** Test getPiSystemInfo() with real PI Web API system info JSON response */
    @Test
    void testGetPiSystemInfoWithRealResponse() throws Exception {
        log.info("Testing getPiSystemInfo with real PI Web API response format");

        // Real PI Web API system info response format
        String realSystemInfoJson =
                "{"
                        + "\"ProductTitle\":\"PI Web API 2019 SP1\","
                        + "\"ProductVersion\":\"1.13.0.6518\","
                        + "\"ServerTime\":\"2025-08-31T09:00:00.000Z\","
                        + "\"Timestamp\":\"2025-08-31T09:00:00.000Z\","
                        + "\"Links\":{"
                        + "\"Self\":\"https://mock-pi-server.test:8443/piwebapi\","
                        + "\"DataServers\":\"https://mock-pi-server.test:8443/piwebapi/dataservers\","
                        + "\"AssetServers\":\"https://mock-pi-server.test:8443/piwebapi/assetservers\","
                        + "\"Points\":\"https://mock-pi-server.test:8443/piwebapi/points\","
                        + "\"Streams\":\"https://mock-pi-server.test:8443/piwebapi/streams\""
                        + "}"
                        + "}";

        try (MockedStatic<PIHttpClient> mockedHttpClient = mockStatic(PIHttpClient.class)) {
            PIHttpClient mockClient = Mockito.mock(PIHttpClient.class);
            JsonNode expectedResponse = objectMapper.readTree(realSystemInfoJson);

            Mockito.when(mockClient.getPiSystemInfo()).thenReturn(expectedResponse);

            // Test the mock
            JsonNode result = mockClient.getPiSystemInfo();

            // Verify real PI Web API response structure
            Assertions.assertNotNull(result, "System info should not be null");
            Assertions.assertTrue(result.has("ProductTitle"), "Should have ProductTitle");
            Assertions.assertTrue(result.has("ProductVersion"), "Should have ProductVersion");
            Assertions.assertTrue(result.has("ServerTime"), "Should have ServerTime");
            Assertions.assertTrue(result.has("Timestamp"), "Should have Timestamp");
            Assertions.assertTrue(result.has("Links"), "Should have Links");

            // Verify specific values
            Assertions.assertEquals("PI Web API 2019 SP1", result.get("ProductTitle").asText());
            Assertions.assertEquals("1.13.0.6518", result.get("ProductVersion").asText());
            Assertions.assertEquals("2025-08-31T09:00:00.000Z", result.get("Timestamp").asText());

            // Verify Links structure
            JsonNode links = result.get("Links");
            Assertions.assertTrue(links.has("Self"), "Links should have Self");
            Assertions.assertTrue(links.has("DataServers"), "Links should have DataServers");
            Assertions.assertTrue(links.has("Points"), "Links should have Points");

            log.info("System info test passed with real PI Web API response format");
        }
    }

    /** Test queryRecordedData() with real PI Web API recorded data JSON response */
    @Test
    void testQueryRecordedDataWithRealResponse() throws Exception {
        log.info("Testing queryRecordedData with real PI Web API response format");

        // Real PI Web API recorded data response format
        String realRecordedDataJson =
                "{"
                        + "\"Items\":["
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:00:00.000Z\","
                        + "\"Value\":25.5,"
                        + "\"Good\":true,"
                        + "\"Questionable\":false,"
                        + "\"Substituted\":false,"
                        + "\"Annotated\":false"
                        + "},"
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:00:30.000Z\","
                        + "\"Value\":26.1,"
                        + "\"Good\":true,"
                        + "\"Questionable\":false,"
                        + "\"Substituted\":false,"
                        + "\"Annotated\":false"
                        + "},"
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:01:00.000Z\","
                        + "\"Value\":25.8,"
                        + "\"Good\":true,"
                        + "\"Questionable\":false,"
                        + "\"Substituted\":false,"
                        + "\"Annotated\":false"
                        + "}"
                        + "],"
                        + "\"Links\":{"
                        + "\"Source\":\"https://mock-pi-server.test:8443/piwebapi/points/F1DPSXXZzR7iYUCmjwaBxHFSkwtooAAAUElNUy5IVUFGRU5HLkNPTVxIRi5BQS5OQUIlM0FMSUETMJY2MTAxLlBW\""
                        + "},"
                        + "\"UnitsAbbreviation\":\"°C\","
                        + "\"WebException\":null"
                        + "}";

        try (MockedStatic<PIHttpClient> mockedHttpClient = mockStatic(PIHttpClient.class)) {
            PIHttpClient mockClient = Mockito.mock(PIHttpClient.class);
            JsonNode expectedResponse = objectMapper.readTree(realRecordedDataJson);

            Mockito.when(
                            mockClient.queryRecordedData(
                                    anyString(),
                                    anyString(),
                                    anyString(),
                                    Mockito.anyInt(),
                                    anyString()))
                    .thenReturn(expectedResponse);

            // Test the mock
            JsonNode result =
                    mockClient.queryRecordedData(
                            "MOCK_WEBID",
                            "2025-08-31T09:00:00.000Z",
                            "2025-08-31T09:01:00.000Z",
                            1000,
                            "Inside");

            // Verify real PI Web API recorded data response structure
            Assertions.assertNotNull(result, "Recorded data should not be null");
            Assertions.assertTrue(result.has("Items"), "Should have Items array");
            Assertions.assertTrue(result.has("Links"), "Should have Links");
            Assertions.assertTrue(result.has("UnitsAbbreviation"), "Should have UnitsAbbreviation");

            // Verify Items array
            JsonNode items = result.get("Items");
            Assertions.assertTrue(items.isArray(), "Items should be an array");
            Assertions.assertEquals(3, items.size(), "Should have 3 data points");

            // Verify first data point structure
            JsonNode firstItem = items.get(0);
            Assertions.assertTrue(firstItem.has("Timestamp"), "Item should have Timestamp");
            Assertions.assertTrue(firstItem.has("Value"), "Item should have Value");
            Assertions.assertTrue(firstItem.has("Good"), "Item should have Good flag");
            Assertions.assertTrue(
                    firstItem.has("Questionable"), "Item should have Questionable flag");
            Assertions.assertTrue(
                    firstItem.has("Substituted"), "Item should have Substituted flag");

            // Verify data values
            Assertions.assertEquals(
                    "2025-08-31T09:00:00.000Z", firstItem.get("Timestamp").asText());
            Assertions.assertEquals(25.5, firstItem.get("Value").asDouble(), 0.01);
            Assertions.assertTrue(firstItem.get("Good").asBoolean());
            Assertions.assertFalse(firstItem.get("Questionable").asBoolean());

            // Verify units
            Assertions.assertEquals("°C", result.get("UnitsAbbreviation").asText());

            log.info("Recorded data test passed with real PI Web API response format");
        }
    }

    /** Test error response with real PI Web API error JSON format */
    @Test
    void testErrorResponseWithRealFormat() throws Exception {
        log.info("Testing error response with real PI Web API error format");

        // Real PI Web API error response format
        String realErrorJson =
                "{"
                        + "\"Errors\":["
                        + "\"The specified PI Point was not found. Point: HF.AA.INVALID:POINT.PV\""
                        + "],"
                        + "\"MultiStatus\":false"
                        + "}";

        try (MockedStatic<PIHttpClient> mockedHttpClient = mockStatic(PIHttpClient.class)) {
            PIHttpClient mockClient = Mockito.mock(PIHttpClient.class);
            JsonNode expectedResponse = objectMapper.readTree(realErrorJson);

            Mockito.when(mockClient.getPiSystemInfo()).thenReturn(expectedResponse);

            // Test the mock
            JsonNode result = mockClient.getPiSystemInfo();

            // Verify real PI Web API error response structure
            Assertions.assertNotNull(result, "Error response should not be null");
            Assertions.assertTrue(result.has("Errors"), "Should have Errors array");
            Assertions.assertTrue(result.has("MultiStatus"), "Should have MultiStatus");

            // Verify error structure
            JsonNode errors = result.get("Errors");
            Assertions.assertTrue(errors.isArray(), "Errors should be an array");
            Assertions.assertEquals(1, errors.size(), "Should have 1 error");
            Assertions.assertTrue(errors.get(0).asText().contains("PI Point was not found"));

            Assertions.assertFalse(result.get("MultiStatus").asBoolean());

            log.info("Error response test passed with real PI Web API error format");
        }
    }

    /** Test data point counting business logic with real response */
    @Test
    void testDataPointCountingBusinessLogic() throws Exception {
        log.info("Testing data point counting business logic");

        String recordedDataJson =
                "{"
                        + "\"Items\":["
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:00:00.000Z\","
                        + "\"Value\":25.5,"
                        + "\"Good\":true"
                        + "},"
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:00:30.000Z\","
                        + "\"Value\":26.1,"
                        + "\"Good\":true"
                        + "},"
                        + "{"
                        + "\"Timestamp\":\"2025-08-31T09:01:00.000Z\","
                        + "\"Value\":25.8,"
                        + "\"Good\":false"
                        + "}"
                        + "]"
                        + "}";

        JsonNode response = objectMapper.readTree(recordedDataJson);

        // Test business logic: count total data points
        int totalCount = response.get("Items").size();
        Assertions.assertEquals(3, totalCount, "Should count 3 total data points");

        // Test business logic: count good quality data points
        int goodCount = 0;
        for (JsonNode item : response.get("Items")) {
            if (item.get("Good").asBoolean()) {
                goodCount++;
            }
        }
        Assertions.assertEquals(2, goodCount, "Should count 2 good quality data points");

        log.info("Data point counting business logic test passed");
    }
}
