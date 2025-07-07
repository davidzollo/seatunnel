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

public class SAPBAPISinkOption extends SAPCommonOption {

    public static final Option<String> BAPI_NAME =
            Options.key("bapi_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the BAPI to be called.");

    public static final Option<String> BAPI_RETURN_TABLE_NAME =
            Options.key("bapi_return_table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the BAPI return table to be used for error handling.");
}
