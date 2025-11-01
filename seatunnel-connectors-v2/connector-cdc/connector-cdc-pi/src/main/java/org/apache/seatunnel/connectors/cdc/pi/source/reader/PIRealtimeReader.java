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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PI real-time data reader - based on WebSocket real-time push mode
 *
 * <p>Handles WebSocket connection, message processing, and buffering for real-time data capture
 * Uses a BlockingQueue to buffer incoming messages from the Netty WebSocket client
 */
public class PIRealtimeReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PIRealtimeReader.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final PIConfigHelper config;
    private PIWebSocketClient piWebSocketClient;
    private final PIWebIdResolver webIdResolver;
    private final PIHttpClient httpClient;
    private final List<String> webIds;
    private final SeaTunnelRowType rowType;

    // Connection status
    private volatile boolean connected = false;

    // Message queue capacity (configurable)
    private final int messageQueueCapacity;

    // Message queue - used to store messages received from Netty WebSocket client
    private final BlockingQueue<SeaTunnelRow> messageQueue;

    // Dropped message counter for monitoring
    private final AtomicLong droppedMessageCount = new AtomicLong(0);

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

        int configuredCapacity = config.getDataBufferQueueSize();

        // Defensive validation: check lower bound (min 1000)
        if (configuredCapacity < 1000) {
            log.warn(
                    "Configured data_buffer_queue_size ({}) too small (min: 1000), fallback to default 300000",
                    configuredCapacity);
            configuredCapacity = 300000;
        }

        // Defensive validation: check upper bound (max 10M to prevent OOM)
        if (configuredCapacity > 10_000_000) {
            log.warn(
                    "Configured data_buffer_queue_size ({}) too large (max: 10,000,000 to prevent OOM), capped to 10M",
                    configuredCapacity);
            configuredCapacity = 10_000_000;
        }

        this.messageQueueCapacity = configuredCapacity;
        this.messageQueue = new LinkedBlockingQueue<>(messageQueueCapacity);
    }

    /**
     * Handle data from the WebSocket message queue and emit to collector.
     *
     * @param output collector supplied by framework
     * @param checkpointLock framework checkpoint lock for consistent emission
     * @return true if any data was processed, false if no data available
     */
    public boolean handleData(Collector<SeaTunnelRow> output, Object checkpointLock)
            throws Exception {
        // Check if WebSocket client is set
        if (piWebSocketClient == null) {
            // log.debug("handleData: piWebSocketClient is null, returning false");
            return false;
        }

        // Check WebSocket connection status
        if (!piWebSocketClient.isConnected()) {
            // Wait for PIWebSocketClient automatic reconnection
            // log.debug("handleData: WebSocket not connected, returning false");
            return false;
        }

        // Update connection status
        if (!connected) {
            connected = true;
            // log.debug("handleData: Connection status updated to connected");
        }

        // Process data from message queue (from Netty WebSocket client)
        boolean hasData = false;
        SeaTunnelRow row;
        while ((row = messageQueue.poll()) != null) {
            if (checkpointLock != null) {
                synchronized (checkpointLock) {
                    output.collect(row);
                }
            } else {
                output.collect(row);
            }
            hasData = true;
        }

        // Return whether data was processed (no more sleep here!)
        return hasData;
    }

    /**
     * Process messages received from Netty WebSocket client
     *
     * @param message WebSocket message content
     */
    public void onWebSocketMessage(String message)
            throws PIConnectorException, JsonProcessingException {
        try {
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
                    // Parse PI Web API data structure and enqueue all data points
                    // log.debug("Processing item: {}", item.path("Name").asText("unknown"));
                    parsePIWebAPIItemAndEnqueue(item);
                }
            } else {
                log.error("Received invalid WebSocket message format (missing Items array)");
            }
        } catch (PIConnectorException e) {
            // Propagate CDC critical errors (queue full, etc) to fail-fast
            throw e;
        } catch (JsonProcessingException e1) {
            log.error("Parse WebSocket message failed: {}", e1.getMessage());
            throw e1;
        } catch (Exception e2) {
            log.error("Error processing WebSocket message: {}", e2.getMessage());
            throw e2;
        }
    }

    /**
     * Parse PI Web API data item and enqueue all data points
     *
     * <p>Data format example: { "WebId": "...", "Name": "811.821-CY-input-consume_sum_day", "Path":
     * "\\\\pims.huafeng.com\\811.821-CY-input-consume_sum_day", "Items": [{ "Timestamp":
     * "2025-06-30T07:30:00Z", "Value": 0.0, "UnitsAbbreviation": "", "Good": true, "Questionable":
     * false, "Substituted": false, "Annotated": false }] }
     *
     * <p>Items array may contain multiple points from reconnection catch-up or high-frequency
     * buffering. All points must be processed for CDC completeness.
     */
    private void parsePIWebAPIItemAndEnqueue(JsonNode itemNode) throws PIConnectorException {
        // Process data point array
        if (itemNode.has("Items") && itemNode.get("Items").isArray()) {
            JsonNode dataItems = itemNode.get("Items");

            if (dataItems.isEmpty()) {
                return;
            }

            // Process ALL data points
            for (JsonNode dataPoint : dataItems) {
                // Use generic data type converter to convert to SeaTunnelRow
                SeaTunnelRow row =
                        PIDataTypeConverter.convertFromJson(
                                itemNode, dataPoint, rowType, config.getJsonField());

                // CDC queue full triggers fail-fast to prevent data loss
                boolean success = messageQueue.offer(row);
                if (!success) {
                    long droppedCount = droppedMessageCount.incrementAndGet();
                    String errorMsg =
                            String.format(
                                    "CDC queue full (capacity: %d), stream: %s, dropped: %d",
                                    messageQueueCapacity,
                                    itemNode.path("Name").asText("unknown"),
                                    droppedCount);
                    log.error(errorMsg);
                    throw new PIConnectorException(PIErrorCode.CDC_QUEUE_BACKPRESSURE, errorMsg);
                }
            }

        } else {
            log.error("Data item missing Items array");
        }
    }

    /** Get the number of messages dropped due to queue being full (for monitoring) */
    public long getDroppedMessageCount() {
        return droppedMessageCount.get();
    }

    /** Get current message queue size (for monitoring) */
    public int getMessageQueueSize() {
        return messageQueue.size();
    }

    /** Get message queue capacity (for monitoring) */
    public int getMessageQueueCapacity() {
        return messageQueueCapacity;
    }

    /**
     * Reset connection status to disconnected state. Called when WebSocket connection is lost.
     * PIWebSocketClient will handle automatic reconnection.
     */
    public void resetConnection() {
        connected = false;
        log.debug(
                "PIRealtimeReader connection status reset, waiting for PIWebSocketClient reconnection");
    }

    @Override
    /** Close the reader and release resources */
    public void close() throws IOException {
        // Only clean up resources owned by this reader
        // WebSocket and HTTP clients are owned by PICDCSourceReader
        // Clear message queue to release memory
        messageQueue.clear();

        // Reset connection status
        connected = false;

        if (piWebSocketClient != null) {
            try {
                piWebSocketClient.stop();
            } catch (Exception e) {
                log.warn("Error while stopping WebSocket client during close", e);
            } finally {
                piWebSocketClient = null;
            }
        }
    }
}
