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

package org.apache.seatunnel.connectors.seatunnel.pi.config;

import org.apache.seatunnel.api.configuration.util.OptionMark;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * PI connection JSON Field Mapping configuration
 *
 * <p>Class for HTTP connection JsonField, configure PI Web API response field JSONPath mapping
 *
 * <p>Support complex PI Web API response fields:
 *
 * <pre>
 * json_field = {
 *   webId = "$.WebId"
 *   name = "$.Name"
 *   path = "$.Path"
 *   pointType = "$.PointType"
 *   engineeringUnits = "$.EngineeringUnits"
 *   descriptor = "$.Descriptor"
 * }
 * </pre>
 */
@Data
@Builder
public class PIJsonField implements Serializable {
    private static final long serialVersionUID = 1L;

    @OptionMark(description = "PI Web API response field JSONPath mapping")
    private Map<String, String> fields;
}
