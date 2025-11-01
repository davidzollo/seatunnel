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

package org.apache.seatunnel.connectors.seatunnel.pi.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/** PI connectionincorrect */
public enum PIErrorCode implements SeaTunnelErrorCode {

    // Configuration errors (PI_CONFIG_001 ~ PI_CONFIG_099)
    CONFIG_INVALID("PI_CONFIG_001", "Configuration item invalid"),
    CONFIG_MISSING_URL("PI_CONFIG_002", "PI Web API URL not configured"),
    CONFIG_MISSING_CREDENTIALS("PI_CONFIG_003", "Authentication information not configured"),
    CONFIG_MISSING_TAG_PATHS("PI_CONFIG_004", "PI Paths or WebIDs not configured"),
    CONFIG_INVALID_TIME_RANGE("PI_CONFIG_005", "Time range configuration invalid"),
    CONFIG_TOO_MANY_WEBIDS("PI_CONFIG_006", "WebID count exceeds limit"),
    CONFIG_INVALID_READ_MODE("PI_CONFIG_007", "Read mode configuration invalid"),
    CONFIG_VALIDATION_FAILED("PI_CONFIG_008", "Configuration validation failed"),
    CONFIG_INVALID_PARALLELISM("PI_CONFIG_009", "WebID count exceeds single task capacity"),

    // Connection errors (PI_CONNECTION_100 ~ PI_CONNECTION_199)
    CONNECTION_FAILED("PI_CONNECTION_100", "Connection to PI Web API failed"),
    CONNECTION_TIMEOUT("PI_CONNECTION_101", "Connection timeout"),
    CONNECTION_REFUSED("PI_CONNECTION_102", "Connection refused"),
    AUTHENTICATION_FAILED("PI_CONNECTION_103", "Authentication failed"),
    SSL_HANDSHAKE_FAILED("PI_CONNECTION_104", "SSL handshake failed"),

    // WebSocket errors (PI_WEBSOCKET_200 ~ PI_WEBSOCKET_299)
    WEBSOCKET_CONNECTION_FAILED("PI_WEBSOCKET_200", "WebSocket connection failed"),
    WEBSOCKET_HANDSHAKE_FAILED("PI_WEBSOCKET_201", "WebSocket handshake failed"),
    WEBSOCKET_CONNECTION_CLOSED("PI_WEBSOCKET_202", "WebSocket connection closed"),
    WEBSOCKET_MESSAGE_PARSE_FAILED("PI_WEBSOCKET_203", "WebSocket message parsing failed"),
    WEBSOCKET_RECONNECT_FAILED("PI_WEBSOCKET_204", "WebSocket reconnection failed"),
    WEBSOCKET_BUFFER_OVERFLOW("PI_WEBSOCKET_205", "WebSocket message buffer overflow"),
    WEBSOCKET_CONNECT_FAILED("PI_WEBSOCKET_206", "WebSocket connection establishment failed"),

    // HTTP request errors (PI_HTTP_300 ~ PI_HTTP_399)
    HTTP_REQUEST_FAILED("PI_HTTP_300", "HTTP request failed"),
    HTTP_RESPONSE_INVALID("PI_HTTP_301", "HTTP response invalid"),
    HTTP_TIMEOUT("PI_HTTP_302", "HTTP request timeout"),
    HTTP_CLIENT_ERROR("PI_HTTP_303", "HTTP client error"),
    HTTP_SERVER_ERROR("PI_HTTP_304", "HTTP server error"),
    CLIENT_ERROR("PI_HTTP_305", "Client error"),
    SERVER_ERROR("PI_HTTP_306", "Server error"),

    // WebID parsing errors (PI_WEBID_400 ~ PI_WEBID_499)
    WEBID_RESOLUTION_FAILED("PI_WEBID_400", "WebID resolution failed"),
    WEBID_NOT_FOUND("PI_WEBID_401", "WebID not found for PI Tag"),
    WEBID_FORMAT_INVALID("PI_WEBID_402", "WebID format invalid"),
    PI_TAG_PATH_INVALID("PI_WEBID_403", "PI Tag Path format invalid"),

    // Data processing errors (PI_DATA_500 ~ PI_DATA_599)
    DATA_PARSE_FAILED("PI_DATA_500", "Data parsing failed"),
    DATA_TYPE_CONVERSION_FAILED("PI_DATA_501", "Data type conversion failed"),
    DATA_SCHEMA_MISMATCH("PI_DATA_502", "Data schema mismatch"),
    DATA_QUALITY_ISSUE("PI_DATA_503", "Data quality issue"),
    DATA_RECOVERY_FAILED("PI_DATA_504", "Data recovery failed"),
    CDC_QUEUE_BACKPRESSURE("PI_DATA_505", "CDC queue backpressure - downstream too slow"),

    // State management errors (PI_STATE_600 ~ PI_STATE_699)
    STATE_SERIALIZATION_FAILED("PI_STATE_600", "State serialization failed"),
    STATE_DESERIALIZATION_FAILED("PI_STATE_601", "State deserialization failed"),
    CHECKPOINT_SAVE_FAILED("PI_STATE_602", "Checkpoint save failed"),
    CHECKPOINT_RESTORE_FAILED("PI_STATE_603", "Checkpoint restore failed"),

    // Split errors (PI_SPLIT_700 ~ PI_SPLIT_799)
    SPLIT_ENUMERATION_FAILED("PI_SPLIT_700", "Split enumeration failed"),
    SPLIT_ASSIGNMENT_FAILED("PI_SPLIT_701", "Split assignment failed"),
    SPLIT_PROCESSING_FAILED("PI_SPLIT_702", "Split processing failed"),

    // General errors (PI_GENERAL_800 ~ PI_GENERAL_899)
    INITIALIZATION_FAILED("PI_GENERAL_800", "Initialization failed"),
    RESOURCE_NOT_AVAILABLE("PI_GENERAL_801", "Resource not available"),
    OPERATION_NOT_SUPPORTED("PI_GENERAL_802", "Operation not supported"),
    INTERNAL_ERROR("PI_GENERAL_803", "Internal error"),
    EXTERNAL_SERVICE_UNAVAILABLE("PI_GENERAL_804", "External service unavailable"),
    READER_INITIALIZATION_FAILED("PI_GENERAL_805", "Reader initialization failed");

    private final String code;
    private final String description;

    PIErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
