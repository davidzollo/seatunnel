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

package org.apache.seatunnel.connectors.seatunnel.pimetadata;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pimetadata.source.PIMetadataSource;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PIMetadataConnectorTest {

    private static final Logger log = LoggerFactory.getLogger(PIMetadataConnectorTest.class);

    @Test
    public void testPIMetadataConnector() {
        log.info("Starting PI Metadata Connector test");

        // Create test configuration
        ReadonlyConfig config = createTestConfig();

        // Test Source creation
        PIMetadataSource source = new PIMetadataSource(config);
        log.info("PI Metadata Source created successfully");

        // Test Schema
        SeaTunnelRowType rowType = source.getProducedType();
        log.info("Schema field count: {}", rowType.getFieldNames().length);
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            log.info("Field {}: {} ({})", i, rowType.getFieldName(i), rowType.getFieldType(i));
        }

        log.info("PI Metadata Connector test completed");
    }

    private ReadonlyConfig createTestConfig() {
        // Create a simple configuration for testing
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("pi_web_api_url", "https://mock-pi-server.test:8443/piwebapi");
        // Automatic judgment type through path, no need to use metadata_type configuration
        configMap.put(
                "pi_paths",
                Arrays.asList(
                        "\\\\mock-pi-server.test\\HF.AA.NAB:LIA-26101.PV",
                        "\\\\mock-pi-server.test\\HF.AA.NAB:LIA-26102.PV"));
        configMap.put("username", "test");
        configMap.put("password", "test");
        configMap.put("validate_ssl", false);

        // Set Schema related configuration - using new columns format
        Map<String, Object> schema = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();

        columns.add(createColumn("webId", "string", "PI WebID"));
        columns.add(createColumn("id", "string", "PI ID"));
        columns.add(createColumn("name", "string", "PI Name"));
        columns.add(createColumn("path", "string", "PI Path"));
        columns.add(createColumn("descriptor", "string", "PI Descriptor"));
        columns.add(createColumn("pointClass", "string", "PI Point Class"));
        columns.add(createColumn("pointType", "string", "PI Point Type"));
        columns.add(createColumn("engineeringUnits", "string", "Engineering Units"));

        schema.put("columns", columns);
        configMap.put("schema", schema);

        // Set JSON field mapping
        Map<String, String> jsonField = new HashMap<>();
        jsonField.put("webId", "$.WebId");
        jsonField.put("id", "$.Id");
        jsonField.put("name", "$.Name");
        jsonField.put("path", "$.Path");
        jsonField.put("descriptor", "$.Descriptor");
        jsonField.put("pointClass", "$.PointClass");
        jsonField.put("pointType", "$.PointType");
        jsonField.put("engineeringUnits", "$.EngineeringUnits");
        configMap.put("json_field", jsonField);

        return ReadonlyConfig.fromMap(configMap);
    }

    private Map<String, Object> createColumn(String name, String type, String comment) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        column.put("nullable", true);
        column.put("comment", comment);
        return column;
    }
}
