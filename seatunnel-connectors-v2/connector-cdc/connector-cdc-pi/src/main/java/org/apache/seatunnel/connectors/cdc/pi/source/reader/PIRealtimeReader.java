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

package org.apache.seatunnel.connectors.cdc.pi.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.pi.source.client.PIWebSocketClient;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIHttpClient;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIWebIdResolver;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PIDataTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/** PI real-time data reader - based on WebSocket real-time push mode */
public class PIRealtimeReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PIRealtimeReader.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final PIConfigHelper config;
    private PIWebSocketClient piWebSocketClient; // Changed to non-final, support delayed setting
    private final PIWebIdResolver webIdResolver;
    private final PIHttpClient httpClient;
    private final List<String> webIds;
    private final SeaTunnelRowType rowType;

    // Connection status
    private boolean connected = false;

    // Heartbeat detection
    private long lastMessageTime = System.currentTimeMillis();
    private final long heartbeatTimeoutMs;

    // Message queue - used to store messages received from Netty WebSocket client
    private final ConcurrentLinkedQueue<SeaTunnelRow> messageQueue = new ConcurrentLinkedQueue<>();

    public PIRealtimeReader(
            PIConfigHelper config,
            PIWebSocketClient piWebSocketClient,
            PIWebIdResolver webIdResolver,
            PIHttpClient httpClient,
            List<String> webIds,
            SeaTunnelRowType rowType) {
        this.config = config;
        this.piWebSocketClient = piWebSocketClient;
        this.webIdResolver = webIdResolver;
        this.httpClient = httpClient;
        this.webIds = webIds;
        this.rowType = rowType;

        // Heartbeat timeout is 5 times the polling interval
        this.heartbeatTimeoutMs = config.getChannelPollingIntervalMs() * 5;
    }

    /** Poll data */
    public void handleData(Collector<SeaTunnelRow> output) throws Exception {
        // Check if WebSocket client is set
        if (piWebSocketClient == null) {
            if (log.isDebugEnabled()) {
                log.debug("WebSocket client not set, waiting for completion...");
            }
            Thread.sleep(100);
            return;
        }

        // Check WebSocket connection status - support two client implementations
        boolean isConnected = isWebSocketConnected();
        boolean isConnecting = isWebSocketConnecting();

        if (!isConnected) {
            // Distinguish between connected and connection failed states
            if (isConnecting) {
                // Connected, silent wait
                if (log.isDebugEnabled()) {
                    log.debug(
                            "WebSocket {} is connecting, waiting for completion...",
                            config.getServerUrl());
                }
                Thread.sleep(200);
                return;
            }

            if (!connected) {
                // Short wait, not recorded as error
                Thread.sleep(500);

                // Check connection status again
                if (!isWebSocketConnected() && !isWebSocketConnecting()) {
                    // Connection neither established nor in progress
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastMessageTime
                            > 30000) { // 30 seconds after last message received
                        log.warn(
                                "WebSocket connection timeout after 30 seconds, attempting reconnect");
                        try {
                            reconnectWebSocket();
                            connected = true;
                            lastMessageTime = currentTime;
                        } catch (Exception e) {
                            log.error("WebSocket {} reconnect failed", config.getServerUrl(), e);
                            Thread.sleep(5000); // Increase wait time to avoid frequent retries
                        }
                    }
                }
            } else {
                // Already tried connecting, waiting for automatic reconnect
                Thread.sleep(1000);
            }
            return;
        }

        // Update connection status
        if (!connected) {
            connected = true;
        }

        // Check heartbeat
        checkHeartbeat();

        // Process data from message queue (from Netty WebSocket client)
        List<SeaTunnelRow> rows = new ArrayList<>();
        SeaTunnelRow row;
        while ((row = messageQueue.poll()) != null) {
            rows.add(row);
        }

        // Process data updates
        for (SeaTunnelRow dataRow : rows) {
            output.collect(dataRow);
            lastMessageTime = System.currentTimeMillis();
        }

        // If no data, sleep briefly to avoid CPU spinning
        if (rows.isEmpty()) {
            Thread.sleep(100);
        }
    }

    /** Check WebSocket connection status - support two client implementations */
    private boolean isWebSocketConnected() {
        return piWebSocketClient != null && piWebSocketClient.isConnected();
    }

    /** Check if WebSocket is connecting */
    private boolean isWebSocketConnecting() {
        return piWebSocketClient != null && piWebSocketClient.isConnecting();
    }

    /** Reconnect WebSocket - support two client implementations */
    private void reconnectWebSocket() throws Exception {
        if (piWebSocketClient != null) {
            // Check current connection status, avoid duplicate operations
            if (piWebSocketClient.isConnected()) {
                return;
            }

            // Netty client reconnects by stop and start
            try {
                log.info("Starting to reconnect WebSocket client to {}", config.getServerUrl());
                piWebSocketClient.stop();
                Thread.sleep(2000); // Increase wait time to ensure connection is fully closed
                piWebSocketClient.start();

            } catch (Exception e) {
                log.error(
                        "Netty WebSocket client reconnect to {} failed", config.getServerUrl(), e);
                throw e;
            }
        } else {
            throw new PIConnectorException(
                    PIErrorCode.CONNECTION_FAILED, "No available WebSocket client for reconnect");
        }
    }

    /** Check heartbeat */
    private void checkHeartbeat() throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastMessageTime > heartbeatTimeoutMs) {
            log.warn(
                    "Heartbeat timeout for WebSocket {}, attempting to reconnect",
                    config.getServerUrl());
            try {
                reconnectWebSocket();
                lastMessageTime = now;
            } catch (Exception e) {
                log.error("Reconnect WebSocket {} failed", config.getServerUrl(), e);
                throw new PIConnectorException(
                        PIErrorCode.WEBSOCKET_RECONNECT_FAILED,
                        "WebSocket reconnect failed: " + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * Process messages received from Netty WebSocket client
     *
     * @param message WebSocket message content
     */
    public void onWebSocketMessage(String message) {
        try {
            // Update heartbeat time
            lastMessageTime = System.currentTimeMillis();

            // Parse message
            JsonNode rootNode = OBJECT_MAPPER.readTree(message);

            // Parse message based on actual data format
            // PI data format: {"Links":{},"Items":[...]}
            if (rootNode.has("Items") && rootNode.get("Items").isArray()) {
                JsonNode items = rootNode.get("Items");

                // Check if it is a PI server heartbeat message (empty Items array)
                if (items.size() == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Received PI server heartbeat message from WebSocket {}, Items is empty, ignoring processing",
                                config.getServerUrl());
                    }
                    return;
                }

                for (JsonNode item : items) {
                    try {
                        // Correctly parse PI Web API data structure
                        SeaTunnelRow dataRow = parsePIWebAPIItem(item);
                        if (dataRow != null) {
                            messageQueue.add(dataRow);
                        }
                    } catch (Exception e) {
                        log.warn(
                                "Parse data update item failed: {}, item content: {}",
                                e.getMessage(),
                                item.toString());
                    }
                }

            } else {
                log.warn(
                        "Received WebSocket message format does not match expectations, does not contain valid Items array: {}",
                        message);
            }
        } catch (JsonProcessingException e) {
            log.error(
                    "Parse WebSocket message failed: {}, message content: {}",
                    e.getMessage(),
                    message);
        } catch (Exception e) {
            log.error("Error processing WebSocket message", e);
        }
    }

    /**
     * Parse PI Web API data item
     *
     * <p>Data format example: { "WebId": "Name": "811.821-CY-input-consume_sum_day", "Path":
     * "\\\\pims.huafeng.com\\811.821-CY-input-consume_sum_day", "Items": [{ "Timestamp":
     * "2025-06-30T07:30:00Z", "Value": 0.0, "UnitsAbbreviation": "", "Good": true, "Questionable":
     * false, "Substituted": false, "Annotated": false }] }
     */
    private SeaTunnelRow parsePIWebAPIItem(JsonNode itemNode) {
        try {
            // Process data point array
            if (itemNode.has("Items") && itemNode.get("Items").isArray()) {
                JsonNode dataItems = itemNode.get("Items");

                // Usually take the latest data point (first)
                if (dataItems.size() > 0) {
                    JsonNode dataPoint = dataItems.get(0);

                    // Use generic data type converter to convert directly to SeaTunnelRow
                    return PIDataTypeConverter.convertFromJson(
                            itemNode, dataPoint, rowType, config.getJsonField());
                }
            }

            log.warn("Data item does not contain valid data point: {}", itemNode.toString());
            return null;

        } catch (Exception e) {
            log.error("Error parsing PI Web API data item: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        // Only clean up resources owned by this reader
        // WebSocket and HTTP clients are owned by PICDCSourceReader

        // Clear message queue to release memory
        if (messageQueue != null) {
            messageQueue.clear();
        }

        // Reset connection status
        connected = false;
    }
}
