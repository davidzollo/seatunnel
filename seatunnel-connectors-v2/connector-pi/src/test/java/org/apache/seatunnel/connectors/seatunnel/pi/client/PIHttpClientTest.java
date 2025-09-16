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

package org.apache.seatunnel.connectors.seatunnel.pi.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

@Disabled("This is an test and requires a running PI Web API instance.")
public class PIHttpClientTest {

    Logger log = LoggerFactory.getLogger(PIHttpClientTest.class);
    private PIConfigHelper configHelper;
    private PIHttpClient piHttpClient;
    private String pointPath;

    @BeforeEach
    public void setup() {
        // --- decoded from runinfo.txt ---
        String username = "WhaleStudio";
        String password = "huafeng#2025";

        pointPath = "\\pims.huafeng.com\\HF.KA.KAA%3AXI-K51005DJR.PV";

        // https://10.89.63.4:8443/piwebapi/points?path=\pims.huafeng.com\HF.KA.KAA%3AXI-K51005DJR.PV

        String url =
                "https://10.89.63.4:8443/piwebapi/streams/F1DPSXXZzR7iYUCmjwaBxHFSkwtooAAAUElNUy5IVUFGRU5HLkNPTVxIRi5LQS5LQUE6WEktSzUxMDA1REpSLlBW/value";

        // Create configuration map
        java.util.Map<String, Object> configMap = new java.util.HashMap<>();
        configMap.put(PIConfig.PI_WEB_API_URL.key(), url);
        configMap.put(PIConfig.USERNAME.key(), username);
        configMap.put(PIConfig.PASSWORD.key(), password);
        configMap.put(PIConfig.AUTH_TYPE.key(), AuthType.BASIC);
        configMap.put(PIConfig.TRUST_ALL_CERTS.key(), true);
        configMap.put(PIConfig.VERIFY_HOSTNAME.key(), false);
        configMap.put(PIConfig.CONNECTION_TIMEOUT_MS.key(), 30000);
        configMap.put(PIConfig.READ_TIMEOUT_MS.key(), 60000);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        configHelper = new PIConfigHelper(readonlyConfig);
    }

    @Test
    public void testGetPiSystemInfo() {
        log.info("--- Starting PIHttpClient integration test ---");
        try {
            // 1. Create client
            piHttpClient = new PIHttpClient(configHelper);
            log.info("PI HTTP client created successfully");

            // 2. Send request to get server information
            log.info("Requesting: " + configHelper.getServerUrl());
            JsonNode serverInfo = piHttpClient.getPiSystemInfo();

            // 3. Validate results
            Assertions.assertNotNull(serverInfo, "Server information should not be null");
            log.info("Successfully get server information: ");
            log.info(serverInfo.toPrettyString());

            Assertions.assertTrue(
                    serverInfo.has("Timestamp"), "Response should contain 'Timestamp'");
            String timestamp = serverInfo.get("Timestamp").asText();
            Assertions.assertNotNull(timestamp);
            log.info("Product title validation successful: " + timestamp);

        } catch (Exception e) {
            log.error("Test failed: " + e.getMessage(), e);
            Assertions.fail("Test failed due to exception", e);
        } finally {
            // 4. Close client
            if (piHttpClient != null) {
                try {
                    piHttpClient.close();
                    log.info("PI HTTP client closed");
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
