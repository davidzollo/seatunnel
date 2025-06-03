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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class SAPBWSourceOption {
    public static final Option<String> APPLICATION_SERVER_HOST =
            Options.key("ashost")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse application server host");
    public static final Option<String> SYSTEM_NUMBER =
            Options.key("sysnr")
                    .stringType()
                    .defaultValue("00")
                    .withDescription("SAP Business Warehouse system number");
    public static final Option<String> CLIENT =
            Options.key("client")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse client");
    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse user");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse password");
    public static final Option<String> LANGUAGE =
            Options.key("lang")
                    .stringType()
                    .defaultValue("EN")
                    .withDescription("SAP Business Warehouse language");

    public static final Option<String> CATEGORY =
            Options.key("category")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse category");
    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse query");
    public static final Option<List<String>> DIMENSIONS_AND_MEASURES =
            Options.key("dimensions_and_measures")
                    .listType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse dimensions and measures of the query");
    public static final Option<String> VARIABLES =
            Options.key("variables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SAP Business Warehouse variables of the query");

    public static final Option<List<QueryTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(QueryTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table list config");
}
