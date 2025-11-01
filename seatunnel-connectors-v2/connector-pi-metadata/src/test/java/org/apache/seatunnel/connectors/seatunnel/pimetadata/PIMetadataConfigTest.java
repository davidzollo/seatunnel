/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.seatunnel.connectors.seatunnel.pimetadata;
//
// import org.apache.seatunnel.api.configuration.ReadonlyConfig;
// import org.apache.seatunnel.connectors.seatunnel.pimetadata.config.PIMetadataConfig;
// import org.apache.seatunnel.connectors.seatunnel.pimetadata.config.PIMetadataParameter;
//
// import org.junit.jupiter.api.Test;
//
// import java.util.Arrays;
// import java.util.HashMap;
// import java.util.Map;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertNotNull;
// import static org.junit.jupiter.api.Assertions.assertTrue;
//
// public class PIMetadataConfigTest {
//
//    @Test
//    public void testPIMetadataParameterConfiguration() {
//        Map<String, Object> configMap = new HashMap<>();
//        configMap.put("pi_web_api_url", "https://10.89.63.4:8443/piwebapi");
//        configMap.put("metadata_type", "POINTS");
//        configMap.put("pi_paths", Arrays.asList("\\\\pims.huafeng.com\\HF.AA.NAB:LIA-26101.PV"));
//        configMap.put("username", "testuser");
//        configMap.put("password", "testpass");
//        configMap.put("batch_size", 50);
//        configMap.put("connect_timeout_ms", 30000);
//        configMap.put("socket_timeout_ms", 60000);
//        configMap.put("retry_count", 3);
//        configMap.put("retry_backoff_ms", 1000);
//        configMap.put("validate_ssl", false);
//
//        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
//        PIMetadataParameter parameter = new PIMetadataParameter();
//        parameter.buildWithConfig(config);
//
//        assertEquals("https://10.89.63.4:8443/piwebapi", parameter.getPiWebApiUrl());
//        assertEquals(PIMetadataConfig.PIMetadataType.POINTS, parameter.getMetadataType());
//        assertEquals(1, parameter.getPiPaths().size());
//        assertEquals("testuser", parameter.getUsername());
//        assertEquals("testpass", parameter.getPassword());
//        assertEquals(50, parameter.getBatchSize());
//        assertEquals(30000, parameter.getConnectTimeoutMs());
//        assertEquals(60000, parameter.getSocketTimeoutMs());
//        assertEquals(3, parameter.getRetryCount());
//        assertEquals(1000, parameter.getRetryBackoffMs());
//        assertEquals(false, parameter.isValidateSsl());
//
//        assertNotNull(parameter.getHeaders());
//        assertTrue(parameter.getHeaders().containsKey("Authorization"));
//        assertTrue(parameter.getHeaders().get("Authorization").startsWith("Basic "));
//    }
//
//    @Test
//    public void testEndpointUrlGeneration() {
//        PIMetadataParameter parameter = new PIMetadataParameter();
//        parameter.setPiWebApiUrl("https://10.89.63.4:8443/piwebapi/");
//
//        parameter.setMetadataType(PIMetadataConfig.PIMetadataType.POINTS);
//        assertEquals("https://10.89.63.4:8443/piwebapi/points", parameter.getEndpointUrl());
//
//        parameter.setMetadataType(PIMetadataConfig.PIMetadataType.ATTRIBUTES);
//        assertEquals("https://10.89.63.4:8443/piwebapi/attributes", parameter.getEndpointUrl());
//    }
//
//    @Test
//    public void testQueryParamsBuilding() {
//        PIMetadataParameter parameter = new PIMetadataParameter();
//        Map<String, String> params =
//                parameter.buildQueryParams(Arrays.asList("path1", "path2", "path3"));
//
//        assertEquals(3, params.size());
//        assertEquals("path1", params.get("path"));
//        assertEquals("path2", params.get("path1"));
//        assertEquals("path3", params.get("path2"));
//    }
// }
