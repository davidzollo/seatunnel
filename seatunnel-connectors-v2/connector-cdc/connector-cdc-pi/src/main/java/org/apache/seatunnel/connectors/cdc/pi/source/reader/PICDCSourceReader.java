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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * PI CDC Source Reader - Real-time data streaming from PI Web API via WebSocket
 *
 * <p>This reader implements CDC for PI systems using WebSocket connections for real-time data
 * streaming. Each split maintains its own WebSocket connection to ensure fault isolation and
 * optimal performance.
 *
 * <p>Key features: - Split-based parallel processing with independent WebSocket connections - Fault
 * tolerance with automatic retry and backoff mechanisms - Production-grade monitoring and metrics
 */
@Slf4j
public class PICDCSourceReader implements SourceReader<SeaTunnelRow, PICDCSplit> {

    private final PIConfigHelper configHelper;
    private final SeaTunnelRowType rowType;
    private final Context readerContext;

    // Core components
    private PIHttpClient httpClient;
    private PIWebIdResolver webIdResolver;

    // Map splitId to the owning split and its active PIRealtimeReader.
    // Once a pending split finishes initialization, we register it here so all runtime
    // operations (data consumption, error handling, shutdown, checkpoint) can locate the reader.
    private final Map<String, SplitAndRealtimeReader> piSplitAndRealtimeReaders =
            new ConcurrentHashMap<>();
    // Holds splits that have been assigned to this subtask but are not fully initialized yet.
    // Newly assigned splits are appended here and initializeNewSplits() scans the list to start
    // WebSocket connections one by one. The entry stays until pendingSplits.removeIf(...) detects
    // the
    // reader is active and removes it.
    private final List<PICDCSplit> pendingSplits = new CopyOnWriteArrayList<>();

    // Track fatal errors for splits that exceeded max retries
    private final Map<String, PIConnectorException> splitFatalErrors = new ConcurrentHashMap<>();

    // Metrics logging rate limiting: prevents log flooding while maintaining observability
    private volatile long lastMetricsLogTime = 0;
    private static final long METRICS_LOG_INTERVAL_MS = 300_000; // 5 minutes

    // Split request throttling: prevents excessive requests when no local pending splits
    private volatile long lastSplitRequestTime = 0;
    private static final long SPLIT_REQUEST_INTERVAL_MS = 5_000; // 5 seconds

    // Reader lifecycle state management
    private volatile boolean running = false;
    private volatile boolean hasMoreSplitsReceived = true;

    public PICDCSourceReader(
            PIConfigHelper configHelper, SeaTunnelRowType rowType, Context readerContext) {
        this.configHelper = configHelper;
        this.rowType = rowType;
        this.readerContext = readerContext;
    }

    /**
     * Initialize PI CDC source reader resources and proactively request initial splits.
     *
     * @throws Exception when initialization fails
     */
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

            // Request initial splits after successful initialization
            requestMoreSplitsIfNeeded();

        } catch (Exception e) {
            log.error("PI CDC data source reader initialization failed", e);
            throw new PIConnectorException(
                    PIErrorCode.READER_INITIALIZATION_FAILED,
                    "PI CDC data source reader initialization failed",
                    e);
        }
    }

    /**
     * Poll next records from all active split readers.
     *
     * @param output collector to emit records
     * @throws Exception if any split processing fails
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

        // Early return if not running
        if (!running) {

            return;
        }

        // Request more splits proactively to avoid throughput lock
        requestMoreSplitsIfNeeded();

        // Check for fatal errors from failed split initializations
        // This catches errors that occurred before splits were added to piSplitAndRealtimeReaders
        if (!splitFatalErrors.isEmpty()) {
            String failedSplitId = splitFatalErrors.keySet().iterator().next();
            PIConnectorException fatalError = splitFatalErrors.get(failedSplitId);
            log.error(
                    "Split {} initialization or processing failed - failing task immediately",
                    failedSplitId);
            throw fatalError;
        }

        boolean hasPendingSplits = !pendingSplits.isEmpty();
        if (hasPendingSplits) {
            // Initialize readers for new splits
            initializeNewSplits();
        }

        // Process real-time data from all split readers
        if (piSplitAndRealtimeReaders.isEmpty()) {
            Thread.sleep(100);
            return;
        }

        // Poll data from all split readers - fail fast on any split error
        // Track whether any split had data this cycle
        boolean hasDataThisCycle = false;
        int activeReaders = 0;
        Object checkpointLock = output.getCheckpointLock();
        for (Map.Entry<String, SplitAndRealtimeReader> entry :
                piSplitAndRealtimeReaders.entrySet()) {
            SplitAndRealtimeReader splitReader = entry.getValue();
            PIRealtimeReader reader = splitReader != null ? splitReader.reader : null;
            if (reader != null) {
                try {
                    PIConnectorException fatalError = splitFatalErrors.get(entry.getKey());
                    if (fatalError != null) {
                        throw fatalError;
                    }
                    // handleData() now returns boolean indicating if data was processed
                    boolean hadData = reader.handleData(output, checkpointLock);
                    if (hadData) {
                        hasDataThisCycle = true;
                    }
                    activeReaders++;
                } catch (InterruptedException e) {
                    // Thread interruption is normal during task shutdown, not a data processing
                    // error
                    log.debug("Split {} processing interrupted during shutdown", entry.getKey());
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    return; // Exit gracefully without failing the task
                } catch (Exception e) {
                    // Split data processing failure should fail the entire task
                    // This ensures users are immediately aware of data processing issues
                    log.error(
                            "Critical failure in split {} data processing - failing task to ensure data integrity",
                            entry.getKey(),
                            e);
                    throw new RuntimeException(
                            String.format("Split %s data processing failed", entry.getKey()), e);
                }
            }
        }

        // Sleep ONCE per pollNext cycle if no data from any split (not per split!)
        // This prevents cumulative sleep delays across splits
        if (!hasDataThisCycle && activeReaders > 0) {
            Thread.sleep(50); // Reduced from 200ms - only sleep once per cycle
        }

        // Health monitoring: Log if no active readers
        if (activeReaders == 0 && !piSplitAndRealtimeReaders.isEmpty()) {
            log.warn(
                    "[HEALTH_CHECK] Reader-{} has {} split readers but none are active - potential connectivity issue",
                    readerContext.getIndexOfSubtask(),
                    piSplitAndRealtimeReaders.size());
        }
    }

    /**
     * Snapshot reader state for checkpointing.
     *
     * @param checkpointId checkpoint identifier
     * @return list of splits (pending + initialized)
     * @throws Exception when snapshot fails
     */
    /**
     * Add newly assigned splits to this reader, filtering out duplicates.
     *
     * @param splits list of splits assigned to this reader
     */
    @Override
    public List<PICDCSplit> snapshotState(long checkpointId) throws Exception {
        // Checkpoint state: Return ALL assigned splits (pending + initialized)
        // This ensures splits can be recovered on task failure
        List<PICDCSplit> stateSplits = new ArrayList<>(pendingSplits);
        for (SplitAndRealtimeReader splitReader : piSplitAndRealtimeReaders.values()) {
            if (splitReader != null) {
                stateSplits.add(splitReader.split);
            }
        }

        log.debug(
                "Checkpoint {}: Snapshotted {} splits for Reader-{} (pending: {}, initialized: {})",
                checkpointId,
                stateSplits.size(),
                readerContext.getIndexOfSubtask(),
                pendingSplits.size(),
                piSplitAndRealtimeReaders.size());

        return stateSplits;
    }

    /**
     * Add newly assigned splits to this reader, filtering out duplicates.
     *
     * @param splits list of splits assigned to this reader
     */
    @Override
    public void addSplits(List<PICDCSplit> splits) {
        synchronized (pendingSplits) {
            if (!splits.isEmpty()) {
                hasMoreSplitsReceived = true;
            }
            Set<String> existingSplitIds = new HashSet<>();
            for (PICDCSplit split : pendingSplits) {
                existingSplitIds.add(split.splitId());
            }

            existingSplitIds.addAll(piSplitAndRealtimeReaders.keySet());

            List<PICDCSplit> newSplits =
                    splits.stream()
                            .filter(split -> !existingSplitIds.contains(split.splitId()))
                            .collect(java.util.stream.Collectors.toList());

            pendingSplits.addAll(newSplits);

            log.info(
                    "Reader {} received {} new splits (filtered {} duplicates), total pending: {}, total initialized: {}",
                    readerContext.getIndexOfSubtask(),
                    newSplits.size(),
                    splits.size() - newSplits.size(),
                    pendingSplits.size(),
                    piSplitAndRealtimeReaders.size());
        }
    }

    /** Signal that no more splits will be assigned for now. */
    @Override
    public void handleNoMoreSplits() {
        // Enumerator signals no more splits available
        // Set flag to false to stop proactive requests until new splits arrive via addSplits()
        synchronized (pendingSplits) {
            hasMoreSplitsReceived = false;
        }
        log.info(
                "Reader-{} received no-more-splits signal with {} pending splits and {} active connections. "
                        + "Stopping proactive requests until new splits available.",
                readerContext.getIndexOfSubtask(),
                pendingSplits.size(),
                piSplitAndRealtimeReaders.size());
        // Update throttle time to avoid immediate re-request
        lastSplitRequestTime = System.currentTimeMillis();
    }

    /**
     * Handle checkpoint completion notification.
     *
     * @param checkpointId checkpoint identifier
     * @throws Exception when flush/ack fails
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug(
                "Checkpoint completed - checkpointId: {}, Reader: {}",
                checkpointId,
                readerContext.getIndexOfSubtask());
    }

    /**
     * Close reader and release all resources.
     *
     * @throws IOException when close fails
     */
    @Override
    public void close() throws IOException {
        running = false;

        try {
            // Close all split readers
            int closedConnections = 0;
            for (Map.Entry<String, SplitAndRealtimeReader> entry :
                    piSplitAndRealtimeReaders.entrySet()) {
                try {
                    SplitAndRealtimeReader splitReader = entry.getValue();
                    if (splitReader != null && splitReader.reader != null) {
                        splitReader.reader.close();
                        closedConnections++;
                        onConnectionReleased(entry.getKey(), "Reader shutdown");
                    }
                } catch (Exception e) {
                    log.error("Error closing reader for split {}", entry.getKey(), e);
                }
            }
            piSplitAndRealtimeReaders.clear();

            // Close HTTP client
            if (httpClient != null) {
                httpClient.close();
            }

            log.info(
                    "PI CDC Reader-{} shutdown completed - {} connections closed, {} pending splits cleared",
                    readerContext.getIndexOfSubtask(),
                    closedConnections,
                    pendingSplits.size());

            lastMetricsLogTime = 0;
            logProductionMetrics();

            // Clear all tracking collections to prevent memory leaks
            splitFatalErrors.clear();
            pendingSplits.clear();
        } catch (Exception e) {
            log.error("Error closing PI CDC data source reader", e);
            throw new IOException("Failed to close PI CDC data source reader", e);
        }
    }

    /**
     * Initialize new split readers with independent WebSocket connections
     *
     * <p>Core logic: 1. Skip already initialized splits to avoid duplicate connections 2. Apply
     * retry backoff for failed splits to prevent infinite retry loops 3. Enforce connection limits
     * to maintain system stability 4. Create isolated WebSocket connection per split for fault
     * tolerance
     *
     * @throws Exception if critical initialization fails
     */
    private void initializeNewSplits() throws Exception {
        int successfulSplits = 0;
        long unprocessedSplits =
                pendingSplits.stream()
                        .filter(split -> !piSplitAndRealtimeReaders.containsKey(split.splitId()))
                        .count();

        if (unprocessedSplits > 0) {
            log.info(
                    "Reader {} initializing splits: {} pending, {} active connections",
                    readerContext.getIndexOfSubtask(),
                    unprocessedSplits,
                    piSplitAndRealtimeReaders.size());
        }

        for (PICDCSplit split : pendingSplits) {
            String splitId = split.splitId();

            // Skip if already initialized
            if (piSplitAndRealtimeReaders.containsKey(splitId)) {
                continue;
            }

            try {
                // Validation: Split size is pre-validated by SplitEnumerator
                // to ensure optimal WebSocket URL length and performance
                int splitWebIdCount = getSplitWebIdCount(split);
                log.debug("Initializing split {} with {} PI paths", splitId, splitWebIdCount);

                // Continue processing all pending splits without artificial limits
                // Each split requires its own WebSocket connection for isolation

                // Create dedicated WebSocket connection for this split
                initializeSplitAndRealtimeReader(split);
                successfulSplits++;

                log.info(
                        "Initialized reader for split {} with {} WebIDs", splitId, splitWebIdCount);

            } catch (Exception e) {
                log.error("Split {} initialization failed", splitId, e);
                throw e;
            }
        }

        // Report results
        if (successfulSplits > 0) {
            log.info(
                    "Reader-{} initialized {} splits, {} active connections",
                    readerContext.getIndexOfSubtask(),
                    successfulSplits,
                    piSplitAndRealtimeReaders.size());
        }

        // Clean up initialized splits from pendingSplits to prevent memory leak
        // This ensures pendingSplits only contains unprocessed splits
        synchronized (pendingSplits) {
            int beforeSize = pendingSplits.size();
            pendingSplits.removeIf(split -> piSplitAndRealtimeReaders.containsKey(split.splitId()));
            int removedCount = beforeSize - pendingSplits.size();
            if (removedCount > 0) {
                log.debug(
                        "Cleaned {} initialized splits from pending list, remaining: {}",
                        removedCount,
                        pendingSplits.size());
            }
        }

        // Production-grade monitoring log
        logProductionMetrics();
    }

    /**
     * Create and initialize PIRealtimeReader for a specific split with its own WebSocket
     *
     * @param split
     * @throws Exception
     */
    private void initializeSplitAndRealtimeReader(PICDCSplit split) throws Exception {
        String splitId = split.splitId();
        PIWebSocketClient splitWebSocketClient = null;
        PIRealtimeReader piSplitRealtimeReader = null;

        try {
            // Resolve WebIDs for this split
            List<String> splitWebIds = resolveSplitWebIds(split);

            // Create WebSocket client for this split
            splitWebSocketClient = PIWebSocketClient.createFromConfig(configHelper, splitWebIds);

            // Create real-time reader for this split (reader will manage the WebSocket client)
            piSplitRealtimeReader =
                    new PIRealtimeReader(
                            configHelper,
                            splitWebSocketClient,
                            webIdResolver,
                            httpClient,
                            splitWebIds,
                            rowType);

            // Setup WebSocket callbacks to connect client with reader
            setupWebSocketCallbacks(splitWebSocketClient, piSplitRealtimeReader, splitId);

            // Start WebSocket connection after callbacks are set
            splitWebSocketClient.start();

            // Only add to map after successful initialization
            piSplitAndRealtimeReaders.put(
                    splitId, new SplitAndRealtimeReader(split, piSplitRealtimeReader));
            splitFatalErrors.remove(splitId);

            log.debug("Created reader for split {} with {} WebIDs", splitId, splitWebIds.size());

        } catch (Exception e) {
            // Clean up resources on failure to prevent leaks
            log.error(
                    "Failed to initialize split reader for {}, cleaning up resources", splitId, e);

            try {
                if (piSplitRealtimeReader != null) {
                    piSplitRealtimeReader.close();
                }
            } catch (Exception cleanupEx) {
                log.warn("Error during reader cleanup for split {}", splitId, cleanupEx);
            }

            try {
                if (splitWebSocketClient != null) {
                    splitWebSocketClient.close();
                }
            } catch (Exception cleanupEx) {
                log.warn("Error during WebSocket cleanup for split {}", splitId, cleanupEx);
            }

            if (e instanceof PIConnectorException) {
                throw e;
            }
            throw new PIConnectorException(
                    PIErrorCode.SPLIT_PROCESSING_FAILED,
                    String.format("Split %s initialization failed", splitId),
                    e);
        }
    }

    /**
     * Check if Reader has capacity for more splits and request them if needed. This enables dynamic
     * multi-split processing based on connection capacity.
     */
    private void requestMoreSplitsIfNeeded() {
        // Calculate unprocessed splits: splits that are not yet initialized
        long unprocessedSplits =
                pendingSplits.stream()
                        .filter(split -> !piSplitAndRealtimeReaders.containsKey(split.splitId()))
                        .count();

        // Only request new splits when all local splits are initialized
        // This ensures balanced distribution across readers and avoids overwhelming the enumerator
        boolean shouldRequest;
        synchronized (pendingSplits) {
            shouldRequest = unprocessedSplits == 0 && running && hasMoreSplitsReceived;
        }

        if (shouldRequest) {
            // Important for fault tolerance: Even when all local splits are initialized,
            // proactively request more splits from Enumerator.
            // This allows the Reader to pick up splits from failed Readers for automatic failover.

            // Throttle requests to avoid overwhelming the Enumerator with empty requests
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastSplitRequestTime >= SPLIT_REQUEST_INTERVAL_MS) {
                try {
                    readerContext.sendSplitRequest();
                    lastSplitRequestTime = currentTime;
                    log.debug(
                            "Reader {} proactively requesting additional splits from Enumerator "
                                    + "(active connections: {}, all local splits initialized)",
                            readerContext.getIndexOfSubtask(),
                            piSplitAndRealtimeReaders.size());
                } catch (Exception e) {
                    log.warn("Failed to proactively request more splits", e);
                }
            }
        }
    }

    /**
     * Setup WebSocket callbacks to connect client with reader.
     *
     * @param webSocketClient WebSocket client
     * @param realtimeReader Real-time reader
     * @param splitId split identifier
     */
    private void setupWebSocketCallbacks(
            PIWebSocketClient webSocketClient, PIRealtimeReader realtimeReader, String splitId) {

        if (webSocketClient == null || realtimeReader == null) {
            throw new IllegalArgumentException("WebSocket client and reader cannot be null");
        }

        // Set message callback - propagate exceptions through splitFatalErrors
        webSocketClient.setOnMessage(
                message -> {
                    try {
                        if (message != null) {
                            realtimeReader.onWebSocketMessage(message);
                        }
                    } catch (PIConnectorException e) {
                        // CDC queue backpressure or other critical errors - store for fail-fast
                        log.error(
                                "Critical error processing WebSocket message for split {}: {}",
                                splitId,
                                e.getMessage());
                        splitFatalErrors.putIfAbsent(splitId, e);
                    } catch (Exception e) {
                        // Unexpected errors - wrap and store
                        log.error(
                                "Unexpected error processing WebSocket message for split {}",
                                splitId,
                                e);
                        PIConnectorException fatalError =
                                new PIConnectorException(
                                        PIErrorCode.WEBSOCKET_MESSAGE_PARSE_FAILED,
                                        String.format(
                                                "Split %s message processing failed: %s",
                                                splitId, e.getMessage()),
                                        e);
                        splitFatalErrors.putIfAbsent(splitId, fatalError);
                    }
                });

        // Set connection success callback
        webSocketClient.setOnOpen(
                () -> {
                    try {
                        log.info(
                                "WebSocket connection established successfully for Reader-{} & split {}",
                                readerContext.getIndexOfSubtask(),
                                splitId);
                        splitFatalErrors.remove(splitId);
                    } catch (Exception e) {
                        log.error("Error in WebSocket onOpen callback for split {}", splitId, e);
                    }
                });

        // Set error callback with enhanced diagnostics for production troubleshooting
        webSocketClient.setOnError(
                error -> {
                    try {
                        // PIWebSocketClient only calls onError when max retries reached
                        // Directly store fatal error to terminate task
                        log.error(
                                "Split {} WebSocket connection failed after maximum retries. Failing task.",
                                splitId,
                                error);

                        PIConnectorException fatalError;
                        if (error instanceof PIConnectorException) {
                            fatalError = (PIConnectorException) error;
                        } else {
                            fatalError =
                                    new PIConnectorException(
                                            PIErrorCode.WEBSOCKET_RECONNECT_FAILED,
                                            String.format(
                                                    "Split %s WebSocket connection failed: %s",
                                                    splitId,
                                                    error != null
                                                            ? error.getMessage()
                                                            : "unknown error"),
                                            error);
                        }

                        splitFatalErrors.putIfAbsent(splitId, fatalError);
                    } catch (Exception e) {
                        log.error("Error in WebSocket onError callback for split {}", splitId, e);
                    }
                });

        // Set close callback with production monitoring
        webSocketClient.setOnClose(
                closeFrame -> {
                    try {
                        if (closeFrame != null) {
                            log.warn(
                                    "WebSocket connection closed for Reader-{} split {}: Code: {}, Reason: {}",
                                    readerContext.getIndexOfSubtask(),
                                    splitId,
                                    closeFrame.statusCode(),
                                    closeFrame.reasonText());
                        } else {
                            log.warn(
                                    "WebSocket connection closed for Reader-{} split {} - No close frame information",
                                    readerContext.getIndexOfSubtask(),
                                    splitId);
                        }

                        // Reset PIRealtimeReader connection status to allow reconnection
                        realtimeReader.resetConnection();
                    } catch (Exception e) {
                        log.error("Error in WebSocket onClose callback for split {}", splitId, e);
                    }
                });
    }

    /**
     * Resolve WebIDs for a specific split.
     *
     * @param split PICDC split
     * @return list of resolved WebIDs
     * @throws Exception when resolution fails
     */
    private List<String> resolveSplitWebIds(PICDCSplit split) throws Exception {
        List<String> splitWebIds = new ArrayList<>();
        int totalPiPaths = 0;

        // Process PI Paths in this split, need to resolve to WebID
        if (split.getPiPaths() != null && !split.getPiPaths().isEmpty()) {
            totalPiPaths += split.getPiPaths().size();
            for (String piPath : split.getPiPaths()) {
                try {
                    String webId = webIdResolver.resolveWebId(piPath);
                    if (webId != null && !webId.isEmpty()) {
                        splitWebIds.add(webId);
                    }
                } catch (Exception e) {
                    log.error("Failed to resolve PI Path {}: {}", piPath, e.getMessage());
                    throw new PIConnectorException(
                            PIErrorCode.WEBID_RESOLUTION_FAILED,
                            String.format(
                                    "Failed to resolve PI Path to WebID: %s, original error: %s",
                                    piPath, e.getMessage()),
                            e);
                }
            }
        }

        // Analyze the failure reason and throw appropriate exception
        if (splitWebIds.isEmpty()) {
            if (totalPiPaths == 0) {
                // No PI Paths configured
                throw new PIConnectorException(
                        PIErrorCode.CONFIG_MISSING_TAG_PATHS,
                        "No valid PI Paths or WebIDs configured in split " + split.splitId());
            } else {
                // PI Paths configured but all resolved to empty WebIDs
                throw new PIConnectorException(
                        PIErrorCode.WEBID_NOT_FOUND,
                        "All configured PI Paths resolved to empty WebIDs for split "
                                + split.splitId());
            }
        }

        log.debug(
                "WebID resolved successfully for split {}, count: {}",
                split.splitId(),
                splitWebIds.size());
        return splitWebIds;
    }

    /**
     * Get WebID count for a split.
     *
     * @param split PICDC split
     * @return number of WebIDs
     */
    private int getSplitWebIdCount(PICDCSplit split) {
        int count = 0;
        if (split.getPiPaths() != null) {
            count += split.getPiPaths().size();
        }
        return count;
    }

    /** Internal holder that keeps split metadata and its realtime reader. */
    private static class SplitAndRealtimeReader {
        final PICDCSplit split;
        final PIRealtimeReader reader;

        SplitAndRealtimeReader(PICDCSplit split, PIRealtimeReader reader) {
            this.split = split;
            this.reader = reader;
        }
    }

    /**
     * Production metrics logging with rate limiting. Logs connection count and queue statistics.
     */
    private void logProductionMetrics() {
        long currentTime = System.currentTimeMillis();

        // Only log if enough time has passed since last metrics log
        if (currentTime - lastMetricsLogTime < METRICS_LOG_INTERVAL_MS) {
            return;
        }

        lastMetricsLogTime = currentTime;

        // Aggregate queue metrics
        int totalQueueSize = 0;
        int totalCapacity = 0;
        long totalDropped = 0;

        for (SplitAndRealtimeReader entry : piSplitAndRealtimeReaders.values()) {
            if (entry != null && entry.reader != null) {
                PIRealtimeReader reader = entry.reader;
                totalQueueSize += reader.getMessageQueueSize();
                totalCapacity += reader.getMessageQueueCapacity();
                totalDropped += reader.getDroppedMessageCount();
            }
        }

        double avgUtilization =
                totalCapacity > 0 ? (double) totalQueueSize / totalCapacity * 100 : 0.0;
        double roundedUtilization = Math.round(avgUtilization * 10.0) / 10.0;

        log.info(
                "PI CDC Reader-{}: Connections={}, QueueUtil={}%, Dropped={}",
                readerContext.getIndexOfSubtask(),
                piSplitAndRealtimeReaders.size(),
                roundedUtilization,
                totalDropped);

        // Alert on high queue utilization
        if (avgUtilization > 90.0) {
            log.warn(
                    "Reader-{} high queue utilization: {}%",
                    readerContext.getIndexOfSubtask(), roundedUtilization);
        }
    }

    /**
     * Called when a connection is released (split completed or failed). Check if we can request
     * more splits to maintain optimal throughput.
     */
    private void onConnectionReleased(String splitId, String reason) {
        log.debug(
                "Connection released for split {} (reason: {}), current connections: {}",
                splitId,
                reason,
                piSplitAndRealtimeReaders.size());

        // Request more splits if we are still running
        if (running) {
            requestMoreSplitsIfNeeded();
        }
    }
}
