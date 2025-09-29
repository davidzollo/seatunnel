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
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.pi.source.client.PIWebSocketClient;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCSplit;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIHttpClient;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIWebIdResolver;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** PI CDC source reader - based on WebSocket real-time push mode */
@Slf4j
public class PICDCSourceReader implements SourceReader<SeaTunnelRow, PICDCSplit> {

    private final PIConfigHelper configHelper;
    private final SeaTunnelRowType rowType;
    private final Context readerContext;

    // Core components
    private PIHttpClient httpClient;
    private PIWebIdResolver webIdResolver;
    private PIWebSocketClient piWebSocketClient;

    // Real-time reader
    private PIRealtimeReader realtimeReader;

    // Split and WebID management
    private final List<PICDCSplit> pendingSplits = new CopyOnWriteArrayList<>();
    private final Map<String, String> piTagToWebIdMap = new ConcurrentHashMap<>();
    private List<String> resolvedWebIds = new CopyOnWriteArrayList<>();

    // State management
    private volatile boolean initialized = false;
    private volatile boolean running = false;

    public PICDCSourceReader(
            PIConfigHelper configHelper, SeaTunnelRowType rowType, Context readerContext) {
        this.configHelper = configHelper;
        this.rowType = rowType;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        log.info(
                "Initialize PI CDC data source reader - Reader {}",
                readerContext.getIndexOfSubtask());
        try {
            // Initialize HTTP client
            this.httpClient = new PIHttpClient(configHelper);
            // Initialize WebID resolver
            this.webIdResolver = new PIWebIdResolver(configHelper, httpClient);
            running = true;

        } catch (Exception e) {
            log.error("PI CDC data source reader initialization failed", e);
            throw new PIConnectorException(
                    PIErrorCode.READER_INITIALIZATION_FAILED,
                    "PI CDC data source reader initialization failed",
                    e);
        }
    }

    @Override
    public void close() throws IOException {

        try {
            // Close real-time reader
            if (realtimeReader != null) {
                realtimeReader.close();
            }

            // Close WebSocket client
            if (piWebSocketClient != null) {
                piWebSocketClient.close();
            }

            // Close HTTP client
            if (httpClient != null) {
                httpClient.close();
            }

            running = false;
            log.info("PI CDC data source reader closed");
        } catch (Exception e) {
            log.error("Error closing PI CDC data source reader", e);
            throw new IOException("Failed to close PI CDC data source reader", e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

        synchronized (output.getCheckpointLock()) {
            // Wait for split assignment
            if (pendingSplits.isEmpty()) {
                Thread.sleep(100);
                return;
            }

            // Initialize reader based on split (only once)
            if (!initialized) {
                try {
                    initializeReaders();
                    initialized = true;
                } catch (Exception e) {
                    // Mark as initialized to prevent infinite retry loop
                    initialized = true;
                    log.error(
                            "PI CDC reader initialization failed, marking as initialized to prevent retry loop",
                            e);
                    throw e;
                }
            }

            // Process real-time data
            if (realtimeReader == null) {
                throw new PIConnectorException(
                        PIErrorCode.READER_INITIALIZATION_FAILED,
                        "Real-time reader not initialized");
            }

            realtimeReader.handleData(output);
        }
    }

    @Override
    public List<PICDCSplit> snapshotState(long checkpointId) throws Exception {
        // Return current assigned splits for checkpoint
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<PICDCSplit> splits) {
        synchronized (pendingSplits) {
            // SplitEnumerator now correctly tracks Reader load, so we trust its assignment
            int currentWebIds = calculateTotalWebIds(pendingSplits);
            int newWebIds = calculateTotalWebIds(splits);
            int totalWebIds = currentWebIds + newWebIds;

            pendingSplits.addAll(splits);

            if (totalWebIds <= 25) {
                log.info(
                        "Reader {} accepted {} splits (normal load), total WebIDs: {}",
                        readerContext.getIndexOfSubtask(),
                        splits.size(),
                        totalWebIds);
            } else if (totalWebIds <= 50) {
                log.warn(
                        "Reader {} accepted {} splits (fault tolerance mode), total WebIDs: {} (above normal 25, but within failover limit 50)",
                        readerContext.getIndexOfSubtask(),
                        splits.size(),
                        totalWebIds);
            } else {
                log.error(
                        "Reader {} accepted {} splits but total WebIDs {} exceeds safe limit 50. "
                                + "This should not happen with corrected SplitEnumerator tracking.",
                        readerContext.getIndexOfSubtask(),
                        splits.size(),
                        totalWebIds);
            }
        }
    }

    /** Calculate total WebIDs count from splits */
    private int calculateTotalWebIds(List<PICDCSplit> splits) {
        return splits.stream().mapToInt(this::calculateSplitWebIds).sum();
    }

    /** Calculate WebIDs count for a single split */
    private int calculateSplitWebIds(PICDCSplit split) {
        int webIdCount = 0;
        if (split.getWebIds() != null) {
            webIdCount += split.getWebIds().size();
        }
        if (split.getPiPaths() != null) {
            webIdCount += split.getPiPaths().size();
        }
        return webIdCount;
    }

    @Override
    public void handleNoMoreSplits() {
        if (pendingSplits.isEmpty()) {
            log.info(
                    "Reader {} has no splits assigned, will remain idle",
                    readerContext.getIndexOfSubtask());
        } else {
            log.info(
                    "Reader {} will process {} assigned splits",
                    readerContext.getIndexOfSubtask(),
                    pendingSplits.size());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug(
                "Checkpoint completed - checkpointId: {}, Reader: {}",
                checkpointId,
                readerContext.getIndexOfSubtask());
    }

    /** Initialize reader based on split (only once) */
    private void initializeReaders() throws Exception {

        try {
            // Resolve WebID
            resolveWebIds();

            // Initialize real-time readerThread-safe initialization sequence:
            // ensure WebSocket client is ready before reader creation
            // 1. First establish WebSocket connection
            initializeWebSocketConnection();

            // 2. Create reader (WebSocket client is ready)
            realtimeReader =
                    new PIRealtimeReader(
                            configHelper,
                            piWebSocketClient, // WebSocket client is initialized
                            webIdResolver,
                            httpClient,
                            resolvedWebIds,
                            rowType);

            log.info("PI CDC reader initialized");

        } catch (Exception e) {
            log.error("PI CDC reader initialization failed", e);
            throw new PIConnectorException(
                    PIErrorCode.READER_INITIALIZATION_FAILED,
                    "PI CDC reader initialization failed",
                    e);
        }
    }

    /** Resolve WebID - based on split assigned WebID/PI Path */
    private void resolveWebIds() throws Exception {
        resolvedWebIds.clear();
        piTagToWebIdMap.clear();

        boolean hasNetworkError = false;
        Exception lastNetworkException = null;
        int totalPiPaths = 0;
        int successfulResolutions = 0;

        // Collect all WebID and PI Path from splits
        for (PICDCSplit split : pendingSplits) {
            // Process direct WebID in split
            if (split.getWebIds() != null && !split.getWebIds().isEmpty()) {
                resolvedWebIds.addAll(split.getWebIds());
            }

            // Process PI Path in split, need to resolve to WebID
            if (split.getPiPaths() != null && !split.getPiPaths().isEmpty()) {
                totalPiPaths += split.getPiPaths().size();
                for (String piPath : split.getPiPaths()) {
                    try {
                        String webId = webIdResolver.resolveWebId(piPath);
                        if (webId != null && !webId.isEmpty()) {
                            resolvedWebIds.add(webId);
                            piTagToWebIdMap.put(piPath, webId);
                            successfulResolutions++;

                        } else {
                            log.warn("PI Path resolved to empty WebID: {}", piPath);
                        }
                    } catch (Exception e) {
                        hasNetworkError = true;
                        lastNetworkException = e;
                        log.error(
                                "Failed to resolve PI Path: {}, error: {}", piPath, e.getMessage());
                    }
                }
                log.debug(
                        "Resolve WebID from PI Path completed, successful: {}/{}",
                        successfulResolutions,
                        totalPiPaths);
            }
        }

        // Analyze the failure reason and throw appropriate exception
        if (resolvedWebIds.isEmpty()) {
            if (hasNetworkError && totalPiPaths > 0) {
                // Network error occurred during resolution
                throw new PIConnectorException(
                        PIErrorCode.WEBID_RESOLUTION_FAILED,
                        "Failed to resolve PI Paths to WebIDs due to network error: "
                                + lastNetworkException.getMessage(),
                        lastNetworkException);
            } else if (totalPiPaths == 0) {
                // No PI Paths configured
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_MISSING_TAG_PATHS,
                        "No valid PI Paths or WebIDs configured in splits");
            } else {
                // PI Paths configured but all resolved to empty WebIDs
                throw new PIConnectorException(
                        PIErrorCode.WEBID_NOT_FOUND,
                        "All configured PI Paths resolved to empty WebIDs - please check PI Path format and server configuration");
            }
        }

        log.info("WebID resolved successfully, total count: {}", resolvedWebIds.size());

        // Validate WebID count to prevent WebSocket URL length issues
        validateWebIdCount();
    }

    /**
     * Validate WebID count to prevent WebSocket URL length issues during checkpoint recovery.
     * Provides clear guidance for resolving the issue without data loss.
     */
    private void validateWebIdCount() {
        final int MAX_SAFE_WEBIDS = 50; // Maximum WebIDs per reader to prevent URL length issues

        if (resolvedWebIds.size() > MAX_SAFE_WEBIDS) {
            String errorMsg =
                    String.format(
                            "Reader received %d WebIDs, which exceeds the safe limit of %d for WebSocket connections. "
                                    + "This typically occurs when recovering from an old checkpoint created with insufficient parallelism. "
                                    + "\n\nTo resolve this issue:\n"
                                    + "1. Increase job parallelism to at least %d (recommended: %d)\n"
                                    + "2. Or reduce the number of PI Paths to %d or fewer\n\n"
                                    + "Current configuration: %d WebIDs with parallelism that allows only %d WebIDs per reader.",
                            resolvedWebIds.size(),
                            MAX_SAFE_WEBIDS,
                            (resolvedWebIds.size() + MAX_SAFE_WEBIDS - 1)
                                    / MAX_SAFE_WEBIDS, // Ceiling division
                            Math.max(
                                    6,
                                    (resolvedWebIds.size() + MAX_SAFE_WEBIDS - 1)
                                            / MAX_SAFE_WEBIDS),
                            MAX_SAFE_WEBIDS,
                            resolvedWebIds.size(),
                            MAX_SAFE_WEBIDS);

            log.error(errorMsg);
            throw new PIConnectorException(PIErrorCode.CONFIG_INVALID, errorMsg);
        }
    }

    /** Initialize WebSocket connection from PISourceReader.initializeWebSocketConnection() */
    private void initializeWebSocketConnection() throws Exception {
        log.debug("Start initializing WebSocket connection");

        try {
            // Create WebSocket client
            piWebSocketClient = PIWebSocketClient.createFromConfig(configHelper, resolvedWebIds);

            // Set WebSocket callbacks
            setupWebSocketCallbacks();
            // Start WebSocket connection
            piWebSocketClient.start();

        } catch (Exception e) {
            log.error("WebSocket connection initialization failed", e);
            throw new PIConnectorException(
                    PIErrorCode.WEBSOCKET_CONNECTION_FAILED,
                    "WebSocket connection initialization failed",
                    e);
        }
    }

    /** Set WebSocket callbacks from PISourceReader.setupWebSocketCallbacks() */
    private void setupWebSocketCallbacks() {
        piWebSocketClient.setOnOpen(
                () -> {
                    log.debug("WebSocket connection established - {}", configHelper.getServerUrl());
                });

        piWebSocketClient.setOnMessage(
                message -> {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Received WebSocket message - length: {}, content: {}",
                                    message.length(),
                                    message);
                        }
                        // Process WebSocket message
                        if (realtimeReader != null) {
                            realtimeReader.onWebSocketMessage(message);
                        } else {
                            // Optimized initialization sequence ensures this should not happen
                            log.error(
                                    "Received WebSocket message but reader is not ready, this should not happen");
                        }
                    } catch (Exception e) {
                        log.error("Error processing WebSocket message", e);
                    }
                });

        piWebSocketClient.setOnClose(
                closeFrame -> {
                    log.warn(
                            "WebSocket connection closed - status code: {}, reason: {}",
                            closeFrame.statusCode(),
                            closeFrame.reasonText());
                });

        piWebSocketClient.setOnError(
                error -> {
                    log.error("WebSocket connection error", error);
                });
    }
}
