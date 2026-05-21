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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public interface KingbaseOptions {

    Option<String> DECODING_PLUGIN_NAME =
            Options.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("pgoutput")
                    .withDescription(
                            "The Kingbase logical decoding protocol used for streaming changes. "
                                    + "Default is pgoutput, which maps to Kingbase built-in syslogical_output on the server. "
                                    + "Explicit decoderbufs is still supported when the server has enabled decoderbufs.");

    Option<String> SLOT_NAME =
            Options.key("slot.name")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription(
                            "The name of the Kingbase logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses this slot "
                                    + "to stream events to the connector that you are configuring. Default is \"seatunnel\".");
}
