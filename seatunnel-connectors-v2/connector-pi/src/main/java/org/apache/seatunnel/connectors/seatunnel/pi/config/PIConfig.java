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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

/**
 * PI Web API Connector Configuration Definition Fully generic design supporting all PI Web API
 * official parameters
 */
public class PIConfig {

    // ================= Basic Connection Configuration =================

    public static final Option<String> PI_WEB_API_URL =
            Options.key("pi_web_api_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Complete PI Web API endpoint address, e.g.: https://ip:8443/piwebapi");

    // ================= Data Point Configuration =================

    public static final Option<List<String>> PI_PATHS =
            Options.key("pi_paths")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "List of PI Paths to read, supports AF Attribute and PI Point paths");

    // Metadata type configuration for PI Metadata connector
    public static final Option<MetadataType> METADATA_TYPE =
            Options.key("metadata_type")
                    .enumType(MetadataType.class)
                    .noDefaultValue()
                    .withDescription(
                            "Metadata type for PI Metadata connector: points (PI Points) or attributes (AF Attributes)");

    // ================= Authentication Configuration =================

    public static final Option<AuthType> AUTH_TYPE =
            Options.key("auth_type")
                    .enumType(AuthType.class)
                    .defaultValue(AuthType.BASIC)
                    .withDescription("Authentication type: Basic, Windows, Bearer");

    public static final Option<String> USERNAME =
            Options.key("username").stringType().noDefaultValue().withDescription("Username");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("Password");

    public static final Option<String> BEARER_TOKEN =
            Options.key("bearer_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bearer Token (used when auth_type=Bearer)");

    // ================= Performance Configuration =================

    public static final Option<Integer> CONNECTION_TIMEOUT_MS =
            Options.key("connection_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout (milliseconds)");

    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Read timeout (milliseconds)");

    public static final Option<Integer> RETRY_ATTEMPTS =
            Options.key("retry_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Retry attempts");

    public static final Option<Long> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Retry backoff multiplier (milliseconds)");

    public static final Option<Long> RETRY_BACKOFF_MAX_MS =
            Options.key("retry_backoff_max_ms")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("Maximum retry backoff time (milliseconds)");

    public static final Option<Integer> KEEP_ALIVE_TIMEOUT_SEC =
            Options.key("keep_alive_timeout_sec")
                    .intType()
                    .defaultValue(180)
                    .withDescription("HTTP Keep-Alive timeout (seconds)");

    // ================= SSL Certificate Configuration =================

    public static final Option<Boolean> TRUST_ALL_CERTS =
            Options.key("trust_all_certs")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to trust all SSL certificates");

    /** SSLhostverificationconfiguration */
    public static final Option<Boolean> VERIFY_HOSTNAME =
            Options.key("verify_hostname")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to verify SSL certificate hostname, set to false to ignore hostname mismatch issues");

    // ================= Real-time Mode Configuration =================

    public static final Option<Boolean> INCLUDE_INITIAL_VALUES =
            Options.key("include_initial_values")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether WebSocket Channel includes initial values");

    public static final Option<Integer> HEARTBEAT_RATE =
            Options.key("heartbeat_rate")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Heartbeat interval (multiple of polling interval)");

    public static final Option<Integer> CHANNEL_POLLING_INTERVAL_MS =
            Options.key("channel_polling_interval_ms")
                    .intType()
                    .defaultValue(3000)
                    .withDescription(
                            "Channel polling interval (milliseconds), matching client 3-second refresh requirement");

    // ================= modeconfiguration =================

    //    public static final Option<Integer> QUERY_WINDOW_MINUTES =
    //            Options.key("query_window_minutes")
    //                    .intType()
    //                    .defaultValue(5)
    //                    .withDescription("Incremental query time window (minutes)");
    //
    //    public static final Option<Integer> OVERLAP_MINUTES =
    //            Options.key("overlap_minutes")
    //                    .intType()
    //                    .defaultValue(1)
    //                    .withDescription("Incremental query overlap time (minutes), avoid data
    // loss");

    // ================= HTTP configuration =================

    public static final Option<Integer> MAX_COUNT =
            Options.key("max_count")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(
                            "Maximum number of records returned per query. If actual data exceeds this limit, only the first N records will be returned and remaining data will be lost.");

    public static final Option<String> BOUNDARY_TYPE =
            Options.key("boundary_type")
                    .stringType()
                    .defaultValue("Inside")
                    .withDescription("Boundary type: Inside, Outside, Interpolated");

    public static final Option<Integer> BATCH_WINDOW_MINUTES =
            Options.key("batch_window_minutes")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Batch processing time window in minutes");

    // ================= Historical Mode Configuration =================

    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Historical data start time (ISO 8601 format)");

    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Historical data end time (ISO 8601 format)");

    // ================= WebSocket Enhanced Configuration (retry) =================

    public static final Option<String> RETRIEVAL_MODE =
            Options.key("retrieval_mode")
                    .stringType()
                    .defaultValue("Auto")
                    .withDescription(
                            "Retrieval mode: Auto (automatically select best value), AtOrBefore(Value at time or previous value), AtOrAfter(Value at time or next value ),Exact (exact time point), Before (nearest value before time point), After (nearest value after time point)");

    public static final Option<Integer> WEBSOCKET_MAX_RETRIES =
            Options.key("websocket_max_retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription("WebSocket maximum reconnection attempts");

    public static final Option<Integer> WEBSOCKET_BUFFER_SIZE =
            Options.key("websocket_buffer_size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("WebSocket message buffer size");

    // WebSocketconnectionwaitconfiguration
    public static final Option<Integer> WEBSOCKET_CONNECTION_WAIT_TIMEOUT_MS =
            Options.key("websocket_connection_wait_timeout_ms")
                    .intType()
                    .defaultValue(120000) // Default 120 seconds
                    .withDescription(
                            "WebSocket connection establishment wait timeout (milliseconds), default 120 seconds. Set to 0 for infinite wait");

    public static final Option<Integer> WEBSOCKET_CONNECTION_STATUS_CHECK_INTERVAL_MS =
            Options.key("websocket_connection_status_check_interval_ms")
                    .intType()
                    .defaultValue(5000) // Default 5 seconds
                    .withDescription(
                            "WebSocket connection status check interval (milliseconds), default 5 seconds");

    public static final Option<Boolean> WEBSOCKET_ALLOW_BACKGROUND_CONNECTION =
            Options.key("websocket_allow_background_connection")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to allow WebSocket to continue connecting in background, default false. If false, exception will be thrown on connection failure");

    // ================= JSON Field Mapping Configuration (HTTP connector design) =================

    @SuppressWarnings("unchecked")
    public static final Option<Map<String, String>> JSON_FIELD =
            (Option<Map<String, String>>)
                    (Option<?>)
                            Options.key("json_field")
                                    .objectType(Map.class)
                                    .noDefaultValue()
                                    .withDescription(
                                            "JSON field mapping configuration, use JSONPath expressions to extract fields from PI responses");

    //    public static final Option<String> CONTENT_FIELD =
    //            Options.key("content_field")
    //                    .stringType()
    //                    .noDefaultValue()
    //                    .withDescription(
    //                            "Content field extraction configuration, use JSONPath expressions
    // to extract arrays or objects from PI responses");

    // ================= Schema Configuration (user data structure) =================

    @SuppressWarnings("unchecked")
    public static final Option<Map<String, Object>> SCHEMA =
            (Option<Map<String, Object>>)
                    (Option<?>)
                            Options.key("schema")
                                    .objectType(Map.class)
                                    .noDefaultValue()
                                    .withDescription(
                                            "User-defined data structure definition, supports all SeaTunnel data types");

    // ================= Batch Mode Configuration (connector-pi specific) =================
    // START_TIME and END_TIME for Historical Mode Configuration

    // ================= Split Configuration (batch mode) =================

    public static final Option<Integer> WEBIDS_PER_SPLIT =
            Options.key("webids_per_split")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Number of PI Paths contained in each split");

    public static final Option<Integer> MAX_SPLITS =
            Options.key("max_splits")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum number of splits");

    public static final Option<Boolean> AUTO_ADJUST_SPLIT_SIZE =
            Options.key("auto_adjust_split_size")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Automatically adjust split size");

    // ================= Batch Resolution Configuration =================

    public static final Option<Integer> WEBID_RESOLVE_BATCH_SIZE =
            Options.key("webid_resolve_batch_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("PI Path to WebID resolution batch size");

    public static final Option<Long> WEBID_RESOLVE_DELAY_MS =
            Options.key("webid_resolve_delay_ms")
                    .longType()
                    .defaultValue(10L)
                    .withDescription("PI Path to WebID resolution interval (milliseconds)");

    /** Maximum supported PI Path quantity */
    public static final int MAX_SUPPORTED_WEBIDS = 100000;

    /** Maximum PI Paths per split for CDC mode to avoid WebSocket URL length limit */
    public static final Option<Integer> MAX_WEBIDS_PER_SPLIT =
            Options.key("max_webids_per_split")
                    .intType()
                    .defaultValue(25)
                    .withDescription(
                            "Maximum number of PI Paths per split for CDC mode (limited by WebSocket URL length)");

    /** Recommended maximum PI Path count for optimal performance */
    public static final Option<Integer> RECOMMENDED_MAX_PI_PATHS =
            Options.key("recommended_max_pi_paths")
                    .intType()
                    .defaultValue(300)
                    .withDescription(
                            "Recommended maximum number of PI Paths for optimal performance. Exceeding this limit may impact performance and increase memory usage.");

    /** Data buffer queue size for PI Source Reader */
    public static final Option<Integer> DATA_BUFFER_QUEUE_SIZE =
            Options.key("data_buffer_queue_size")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "Size of the internal data buffer queue for PI Source Reader. Larger values provide better throughput but consume more memory.");

    /** Batch size for draining data from buffer queue */
    public static final Option<Integer> BATCH_DRAIN_SIZE =
            Options.key("batch_drain_size")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Number of records to drain from buffer queue in each batch. Higher values improve throughput but may increase memory usage.");

    /** Buffer low threshold for triggering next batch fetch */
    public static final Option<Integer> BUFFER_LOW_THRESHOLD =
            Options.key("buffer_low_threshold")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "When buffer size falls below this threshold, trigger fetching next batch of data. Higher values provide better prefetching but consume more memory.");
}
