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

package org.apache.seatunnel.connectors.seatunnel.sapbw.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryTableConfig implements Serializable {

    @JsonProperty("query")
    private String query;

    @JsonProperty("category")
    private String category;

    @JsonProperty("dimensions_and_measures")
    private List<String> dimensionsAndMeasures;

    @JsonProperty("variables")
    private String variables;

    @Tolerate
    public QueryTableConfig() {}

    public static Map<TablePath, QueryTableConfig> of(ReadonlyConfig connectorConfig) {
        List<QueryTableConfig> tableList;
        if (connectorConfig.getOptional(SAPBWSourceOption.TABLE_LIST).isPresent()) {
            tableList = connectorConfig.get(SAPBWSourceOption.TABLE_LIST);
        } else {
            QueryTableConfig tableProperty =
                    QueryTableConfig.builder()
                            .category(connectorConfig.get(SAPBWSourceOption.CATEGORY))
                            .query(connectorConfig.get(SAPBWSourceOption.QUERY))
                            .dimensionsAndMeasures(
                                    connectorConfig.get(SAPBWSourceOption.DIMENSIONS_AND_MEASURES))
                            .variables(connectorConfig.get(SAPBWSourceOption.VARIABLES))
                            .build();
            tableList = Collections.singletonList(tableProperty);
        }

        if (tableList.size() > 1) {
            Set<String> queriesSet =
                    tableList.stream().map(QueryTableConfig::getQuery).collect(Collectors.toSet());
            if (queriesSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_path`, not allow null/duplicate table path: "
                                + queriesSet);
            }
        }
        return tableList.stream()
                .collect(
                        Collectors.toMap(
                                tableConfig ->
                                        TablePath.of(
                                                tableConfig.getCategory(), tableConfig.getQuery()),
                                tableConfig -> tableConfig));
    }
}
