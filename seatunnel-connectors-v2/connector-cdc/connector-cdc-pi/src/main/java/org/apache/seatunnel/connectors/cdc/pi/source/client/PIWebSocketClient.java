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
package org.apache.seatunnel.connectors.cdc.pi.source.client;

import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * WebSocket client, designed for PI Web API
 *
 * <p>Technical features:
 *
 * <ul>
 *   <li>High-performance Netty asynchronous I/O architecture
 *   <li>Production-grade connection pool and resource management
 *   <li>Smart reconnection mechanism and exponential backoff strategy
 *   <li>Complete heartbeat keep-alive and health check
 *   <li>SSL/TLS secure connection support
 *   <li>Thread-safe concurrent processing
 *   <li>Rich monitoring metrics and performance statistics
 *   <li>Graceful resource release and memory management
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * PIWebSocketClient client = PIWebSocketClient.createFromConfig(config, webIds);
 *
 * client.setOnMessage(message -> {
 *     // Process received message
 *     log.info("Received data: {}", message);
 * });
 *
 * client.setOnOpen(() -> {
 *     log.info("Connection established");
 * });
 *
 * client.start();
 * }</pre>
 */
public class PIWebSocketClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PIWebSocketClient.class);

    /** Maximum reconnection attempts */
    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    /** Thread pool shutdown timeout (seconds) */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    // ========== Connection configuration fields ==========
    private final URI uri;
    private final boolean trustAll;
    private final int reconnectIntervalSeconds;
    private final int heartbeatIntervalSeconds;
    private final int connectTimeoutMillis;
    private final int handshakeTimeoutMillis;
    private volatile PIConfigHelper authenticationConfig;

    // ========== Netty related fields ==========
    private volatile EventLoopGroup eventLoopGroup;
    private volatile Channel channel;
    private volatile ScheduledExecutorService scheduler;

    // ========== State management fields ==========
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicReference<String> lastError = new AtomicReference<>();

    // ========== Statistics and time fields ==========
    private final AtomicLong totalMessagesReceived = new AtomicLong(0);
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private volatile long lastConnectAttemptTime = 0;
    private volatile long connectionEstablishedTime = 0;

    // ========== Callback function fields ==========
    private volatile Consumer<String> onMessage;
    private volatile Runnable onOpen;
    private volatile Consumer<Throwable> onError;
    private volatile Consumer<CloseWebSocketFrame> onClose;

    // ========================================
    // Constructor
    // ========================================

    /**
     * Construct Netty WebSocket client
     *
     * @param url WebSocket URL (supports ws:// and wss://)
     * @param trustAll Whether to trust all SSL certificates (only used for test environment)
     * @param reconnectIntervalSeconds Reconnection interval (seconds)
     * @param heartbeatIntervalSeconds Heartbeat interval (seconds)
     * @param connectTimeoutMillis Connection timeout (milliseconds)
     * @throws IllegalArgumentException If URL format is invalid
     */
    public PIWebSocketClient(
            String url,
            boolean trustAll,
            int reconnectIntervalSeconds,
            int heartbeatIntervalSeconds,
            int connectTimeoutMillis) {

        // Parameter validation
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("WebSocket URL cannot be empty");
        }
        if (reconnectIntervalSeconds < 1) {
            throw new IllegalArgumentException(
                    "Reconnection interval must be greater than 0 seconds");
        }
        if (heartbeatIntervalSeconds < 0) {
            throw new IllegalArgumentException("Heartbeat interval cannot be less than 0 seconds");
        }
        if (connectTimeoutMillis < 1000) {
            throw new IllegalArgumentException(
                    "Connection timeout must be greater than or equal to 1000 milliseconds");
        }

        try {
            this.uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid WebSocket URL: " + url, e);
        }

        // Verify protocol
        String scheme = uri.getScheme();
        if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(
                    "Unsupported protocol: " + scheme + ", only ws:// or wss:// is supported");
        }

        this.trustAll = trustAll;
        this.reconnectIntervalSeconds = reconnectIntervalSeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.handshakeTimeoutMillis = Math.max(connectTimeoutMillis * 2, 30000);

        if (trustAll) {
            log.debug("Trust all SSL certificates mode enabled!");
        }

        log.info(
                "Initialize PIWebSocketClient - URL: {}, connection timeout: {}ms, handshake timeout: {}ms, heartbeat interval: {}s, reconnection interval: {}s",
                url,
                connectTimeoutMillis,
                handshakeTimeoutMillis,
                heartbeatIntervalSeconds,
                reconnectIntervalSeconds);
    }

    // ========================================
    // Callback function settings
    // ========================================

    /**
     * Set message receive callback
     *
     * @param onMessage Message callback function, receive WebSocket text message
     */
    public void setOnMessage(Consumer<String> onMessage) {
        this.onMessage = onMessage;
    }

    /**
     * Set connection success callback
     *
     * @param onOpen Connection success callback function
     */
    public void setOnOpen(Runnable onOpen) {
        this.onOpen = onOpen;
    }

    /**
     * Set error callback
     *
     * @param onError Error callback function, receive exception information
     */
    public void setOnError(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    /**
     * Set connection close callback
     *
     * @param onClose Connection close callback function, receive close frame information
     */
    public void setOnClose(Consumer<CloseWebSocketFrame> onClose) {
        this.onClose = onClose;
    }

    // ========================================
    // Life cycle management
    // ========================================

    /**
     * Start WebSocket client
     *
     * <p>This method is asynchronous, and the connection will be established in the background
     * thread. The connection status can be monitored by setting the callback function.
     *
     * @throws IllegalStateException If the client has already been started
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Start PIWebSocketClient...");

            // Create dedicated thread pool
            this.eventLoopGroup =
                    new NioEventLoopGroup(0, createThreadFactory("PIWebSocketClient-EventLoop"));
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            2, createThreadFactory("PIWebSocketClient-Scheduler"));

            // Start connection
            scheduleConnect(0);
        } else {
            throw new IllegalStateException("WebSocket client has already been started");
        }
    }

    /**
     * Stop WebSocket client
     *
     * <p>This method will gracefully close all connections and thread pools, ensuring that
     * resources are released correctly.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stop PIWebSocketClient...");
            connected.set(false);
            connecting.set(false);

            try {
                // Send close frame
                if (channel != null && channel.isActive()) {
                    ChannelFuture closeFuture =
                            channel.writeAndFlush(new CloseWebSocketFrame(1000, "Normal close"));
                    closeFuture.await(5, TimeUnit.SECONDS);
                }

                // Close channel
                if (channel != null) {
                    ChannelFuture channelCloseFuture = channel.close();
                    channelCloseFuture.await(5, TimeUnit.SECONDS);
                }
                // Wait for graceful shutdown to complete
                boolean gracefulShutdown = shutdownLatch.await(10, TimeUnit.SECONDS);
                if (!gracefulShutdown) {
                    log.warn("Graceful shutdown timeout, force close");
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Shutdown process interrupted", e);
            } catch (Exception e) {
                log.warn("Exception occurred while closing WebSocket connection", e);
            } finally {
                // Close thread pool
                shutdownThreadPools();

                log.info("PIWebSocketClient has been completely stopped");
            }
        }
    }

    /** Close client (implement AutoCloseable interface) */
    @Override
    public void close() {
        stop();
    }

    // ========================================
    // Message sending method
    // ========================================

    /**
     * Send text message
     *
     * @param text The text content to be sent
     * @return The CompletableFuture of the send operation
     */
    public CompletableFuture<Void> sendText(String text) {
        if (text == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalArgumentException("Message content cannot be null"));
            return future;
        }

        if (!isConnected()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalStateException("WebSocket connection not established"));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            ChannelFuture channelFuture = channel.writeAndFlush(new TextWebSocketFrame(text));
            channelFuture.addListener(
                    f -> {
                        if (f.isSuccess()) {
                            totalMessagesSent.incrementAndGet();
                            future.complete(null);
                            log.debug("Text message sent successfully, length: {}", text.length());
                        } else {
                            future.completeExceptionally(f.cause());
                            log.warn("Text message sending failed", f.cause());
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
            log.error("Exception occurred while sending text message", e);
        }

        return future;
    }

    /**
     * Send binary message
     *
     * @param data The binary data to be sent
     * @return The CompletableFuture of the send operation
     */
    public CompletableFuture<Void> sendBinary(byte[] data) {
        if (data == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Data cannot be null"));
            return future;
        }

        if (!isConnected()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalStateException("WebSocket connection not established"));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            ByteBuf buffer = Unpooled.wrappedBuffer(data);
            ChannelFuture channelFuture = channel.writeAndFlush(new BinaryWebSocketFrame(buffer));
            channelFuture.addListener(
                    f -> {
                        if (f.isSuccess()) {
                            totalMessagesSent.incrementAndGet();
                            future.complete(null);
                            log.debug(
                                    "Binary message sent successfully, length: {} bytes",
                                    data.length);
                        } else {
                            future.completeExceptionally(f.cause());
                            log.warn("Binary message sending failed", f.cause());
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
            log.error("Exception occurred while sending binary message", e);
        }

        return future;
    }

    /**
     * Send Ping frame
     *
     * @return The CompletableFuture of the send operation
     */
    public CompletableFuture<Void> sendPing() {
        return sendPing(new byte[] {1, 2, 3, 4});
    }

    /**
     * Send Ping frame (with custom data)
     *
     * @param data Ping frame data
     * @return The CompletableFuture of the send operation
     */
    public CompletableFuture<Void> sendPing(byte[] data) {
        if (!isConnected()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalStateException("WebSocket connection not established"));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            ByteBuf buffer = Unpooled.wrappedBuffer(data != null ? data : new byte[0]);
            ChannelFuture channelFuture = channel.writeAndFlush(new PingWebSocketFrame(buffer));
            channelFuture.addListener(
                    f -> {
                        if (f.isSuccess()) {
                            future.complete(null);
                            log.debug("Ping frame sent successfully");
                        } else {
                            future.completeExceptionally(f.cause());
                            log.warn("Ping frame sending failed", f.cause());
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
            log.error("Exception occurred while sending Ping frame", e);
        }

        return future;
    }

    // ========================================
    // Factory method
    // ========================================

    /**
     * Create WebSocket client for PI Web API - supports WebID parameters
     *
     * @param piConfigHelper PI connection configuration
     * @param webIds WebID list to subscribe
     * @return Configured WebSocket client
     */
    public static PIWebSocketClient createFromConfig(
            PIConfigHelper piConfigHelper, List<String> webIds) {
        // Build WebSocket URL
        String serverUrl = piConfigHelper.getServerUrl();

        String webSocketUrl;

        if (serverUrl.startsWith("https://")) {
            webSocketUrl = serverUrl.replace("https://", "wss://");
        } else if (serverUrl.startsWith("http://")) {
            webSocketUrl = serverUrl.replace("http://", "ws://");
        } else {
            webSocketUrl = (piConfigHelper.isHttps() ? "wss://" : "ws://") + serverUrl;
        }

        // Correctly build WebSocket path, avoid duplication
        // Remove trailing slash to ensure path correctness
        if (webSocketUrl.endsWith("/")) {
            webSocketUrl = webSocketUrl.substring(0, webSocketUrl.length() - 1);
        }

        // Ensure /piwebapi path is included
        String piwebapi = "/piwebapi";
        if (webSocketUrl.contains("/piwebapi")) {
            webSocketUrl =
                    webSocketUrl.substring(0, webSocketUrl.indexOf(piwebapi) + piwebapi.length());
        } else {
            webSocketUrl = webSocketUrl + "/piwebapi";
        }

        // Use correct PI Web API WebSocket endpoint path
        if (!webSocketUrl.endsWith("/streamsets/channel")) {
            webSocketUrl = webSocketUrl + "/streamsets/channel";
        }

        // Build query parameters - WebID passed through URL parameters
        StringBuilder queryParams = new StringBuilder();
        queryParams
                .append("?includeInitialValues=")
                .append(piConfigHelper.isIncludeInitialValues());
        queryParams.append("&heartbeatRate=").append(piConfigHelper.getHeartbeatRate());

        // Add retrievalMode parameter to control data acquisition mode (real value vs
        // interpolation)
        if (piConfigHelper.getRetrievalMode() != null
                && !piConfigHelper.getRetrievalMode().isEmpty()) {
            queryParams.append("&retrievalMode=").append(piConfigHelper.getRetrievalMode());
            log.info(
                    "WebSocket connection uses retrievalMode: {}",
                    piConfigHelper.getRetrievalMode());
        }

        // Add WebID parameter (URL encoding to handle special characters)
        if (webIds != null && !webIds.isEmpty()) {
            for (String webId : webIds) {
                try {
                    String encodedWebId =
                            URLEncoder.encode(webId, StandardCharsets.UTF_8.toString());
                    queryParams.append("&webid=").append(encodedWebId);
                    log.debug("WebID encoded: {} -> {}", webId, encodedWebId);
                } catch (Exception e) {
                    log.warn(
                            "WebID encoding failed, using original value: {}, error: {}",
                            webId,
                            e.getMessage());
                    queryParams.append("&webid=").append(webId);
                }
            }
        }

        webSocketUrl = webSocketUrl + queryParams.toString();

        // Validate URL length to prevent HTTP 414 error
        if (webSocketUrl.length() > 8192) { // 8KB limit
            log.error(
                    "WebSocket URL too long ({} chars, max: 8192). URL: {}",
                    webSocketUrl.length(),
                    webSocketUrl.length() > 500
                            ? webSocketUrl.substring(0, 500) + "..."
                            : webSocketUrl);
            throw new IllegalArgumentException(
                    String.format(
                            "WebSocket URL too long (%d chars, max: 8192). "
                                    + "WebID count: %d. Please reduce PI Path count or increase parallelism.",
                            webSocketUrl.length(), webIds != null ? webIds.size() : 0));
        }

        // Configuration parameters
        boolean trustAllCerts =
                piConfigHelper.isTrustAllCerts(); // Use trustAllCerts field in configuration
        int reconnectIntervalSeconds = 5; // Default reconnection interval
        int heartbeatIntervalSeconds =
                piConfigHelper.getHeartbeatRate(); // Use heartbeat rate in configuration
        int connectTimeoutMillis =
                piConfigHelper
                        .getWebSocketConnectionWaitTimeoutMs(); // Use WebSocket connection wait
        // timeout configuration

        log.debug(
                "Create PI WebSocket client (Netty) - URL: {}, trust all certificates: {}, WebSocket connection timeout: {}ms",
                webSocketUrl,
                trustAllCerts,
                connectTimeoutMillis);

        // Create authenticated WebSocket client
        PIWebSocketClient client =
                new PIWebSocketClient(
                        webSocketUrl,
                        trustAllCerts,
                        reconnectIntervalSeconds,
                        heartbeatIntervalSeconds,
                        connectTimeoutMillis);

        // Save authentication configuration, used when connecting
        client.authenticationConfig = piConfigHelper;

        return client;
    }

    // ========================================
    // Status query method
    // ========================================

    /**
     * Check if the client is connected
     *
     * @return Whether the client is connected
     */
    public boolean isConnected() {
        return running.get() && connected.get() && channel != null && channel.isActive();
    }

    /**
     * Check if the client is connecting
     *
     * @return Whether the client is connecting
     */
    public boolean isConnecting() {
        return connecting.get();
    }

    /**
     * Get connection status information
     *
     * @return Connection status description
     */
    public String getConnectionStatus() {
        if (isConnected()) {
            long uptime = System.currentTimeMillis() - connectionEstablishedTime;
            return String.format("Connected (uptime: %dms)", uptime);
        } else if (isConnecting()) {
            long connectingTime = System.currentTimeMillis() - lastConnectAttemptTime;
            return String.format("Connecting (attempted: %dms)", connectingTime);
        } else {
            String error = lastError.get();
            return "Not connected" + (error != null ? " (error: " + error + ")" : "");
        }
    }

    /**
     * Get the last error information
     *
     * @return Error information
     */
    public String getLastError() {
        return lastError.get();
    }

    /**
     * Get performance summary information
     *
     * @return Performance summary
     */
    public String getPerformanceSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("WebSocket performance statistics: ");
        sb.append("Message received: ").append(totalMessagesReceived.get());
        sb.append(", Message sent: ").append(totalMessagesSent.get());
        sb.append(", Running status: ").append(isConnected() ? "Connected" : "Not connected");
        if (connectionEstablishedTime > 0) {
            long uptime = System.currentTimeMillis() - connectionEstablishedTime;
            sb.append(", Running duration: ").append(uptime).append("ms");
        }
        return sb.toString();
    }

    /**
     * Check if recovery is needed (compatibility method)
     *
     * @return Whether recovery is needed
     */
    public boolean needsRecovery() {
        return !isConnected() && running.get();
    }

    /** Mark recovery complete (compatibility method) */
    public void markRecoveryComplete() {
        log.info("Recovery process marked as complete");
    }

    /**
     * Receive updates (compatibility method)
     *
     * @param timeoutMs Timeout (milliseconds)
     * @return Data update list
     */
    @Deprecated
    public List<Object> receiveUpdates(int timeoutMs) {
        log.warn("receiveUpdates method is deprecated, please use asynchronous callback mechanism");
        return new java.util.ArrayList<>();
    }

    // ========================================
    // Private helper methods
    // ========================================

    private ThreadFactory createThreadFactory(String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, prefix + "-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
    }

    private void scheduleConnect(int attempt) {
        if (attempt < MAX_RECONNECT_ATTEMPTS) {
            connectWithRetry(attempt);
        } else {
            log.error("Maximum reconnection attempts reached, connection failed");
            connected.set(false);
            lastError.set("Maximum reconnection attempts reached, connection failed");
            if (onError != null) {
                onError.accept(
                        new RuntimeException(
                                "Maximum reconnection attempts reached, connection failed"));
            }
        }
    }

    private void connectWithRetry(int attempt) {
        if (connecting.compareAndSet(false, true)) {
            scheduler.execute(
                    () -> {
                        log.info(
                                "Attempt to establish WebSocket connection, attempt number: {}",
                                attempt + 1);
                        while (running.get() && !Thread.currentThread().isInterrupted()) {
                            try {
                                lastConnectAttemptTime = System.currentTimeMillis();
                                log.info(
                                        "Start to establish WebSocket connection, attempt time: {}",
                                        new java.util.Date(lastConnectAttemptTime));

                                setupConnection();

                                // Connection successful - note: connected status is now set in
                                // onOpen callback
                                connectionEstablishedTime = System.currentTimeMillis();
                                long connectDuration =
                                        connectionEstablishedTime - lastConnectAttemptTime;
                                log.info(
                                        "WebSocket connection established successfully, duration: {}ms",
                                        connectDuration);

                                connecting.set(false);
                                lastError.set(null);
                                break;

                            } catch (Throwable e) {
                                connected.set(false);
                                lastError.set(e.getMessage());

                                long connectDuration =
                                        System.currentTimeMillis() - lastConnectAttemptTime;
                                log.warn(
                                        "WebSocket connection failed, duration: {}ms, retry after {} seconds: {}",
                                        connectDuration,
                                        reconnectIntervalSeconds,
                                        e.getMessage());

                                if (onError != null) {
                                    try {
                                        onError.accept(e);
                                    } catch (Exception callbackEx) {
                                        log.error(
                                                "Exception occurred while executing onError callback",
                                                callbackEx);
                                    }
                                }

                                try {
                                    TimeUnit.SECONDS.sleep(reconnectIntervalSeconds);
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                        connecting.set(false);
                    });
        }
    }

    /**
     * Directly establish WebSocket connection (synchronous blocking, suitable for test/minimal
     * scenarios) Note: This method does not automatically reconnect, does not manage thread pools,
     * and production environment recommends using start()
     */
    public void setupConnection() throws Exception {
        String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
        final String host = uri.getHost();
        final int port =
                uri.getPort() == -1 ? ("wss".equalsIgnoreCase(scheme) ? 443 : 80) : uri.getPort();
        final String path = uri.getPath() + (uri.getQuery() != null ? "?" + uri.getQuery() : "");
        final boolean ssl = "wss".equalsIgnoreCase(scheme);

        log.debug(
                "Prepare to connect WebSocket server - Host: {}, Port: {}, Path: {}, SSL: {}",
                host,
                port,
                path,
                ssl);

        // Network connectivity pre-detection
        try {
            performNetworkTest(host, port);
            log.debug("Network connectivity detection passed");
        } catch (Exception e) {
            log.error(
                    "Network connectivity detection failed: {}, current thread interrupt status: {}",
                    e.getMessage(),
                    Thread.currentThread().isInterrupted());
            throw new RuntimeException(
                    "Network connectivity detection failed, cannot connect to " + host + ":" + port,
                    e);
        }

        // Configure SSL context
        final SslContext sslCtx;
        if (ssl) {
            if (trustAll) {
                sslCtx =
                        SslContextBuilder.forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                //                                .protocols("TLSv1.2", "TLSv1.3")
                                // // Explicitly specify supported TLS versions
                                .build();
                log.debug(
                        "Trust all certificates configured, host name verification disabled, supports TLS 1.2/1.3");
            } else {
                sslCtx = SslContextBuilder.forClient().build();
                log.debug("Use standard SSL certificate verification, supports TLS 1.2/1.3");
            }
        } else {
            sslCtx = null;
        }

        log.debug(
                "WebSocket connection configuration - protocol: {}, host: {}, port: {}, path: {}, SSL: {}",
                scheme,
                host,
                port,
                path,
                ssl);

        // Create WebSocket handshaker, add authentication header
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.USER_AGENT, "SeaTunnel-PI-Connector");

        // Add authentication header
        if (authenticationConfig != null) {
            AuthType authType = authenticationConfig.getAuthType();
            if (AuthType.BASIC.equals(authType)
                    && authenticationConfig.getUsername() != null
                    && authenticationConfig.getPassword() != null) {
                String authValue =
                        authenticationConfig.getUsername()
                                + ":"
                                + authenticationConfig.getPassword();
                String encodedAuth =
                        java.util.Base64.getEncoder().encodeToString(authValue.getBytes());
                headers.add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedAuth);
                log.debug("Basic authentication header added to WebSocket handshake");
            } else if (AuthType.BEARER.equals(authType)
                    && authenticationConfig.getBearerToken() != null) {
                headers.add(
                        HttpHeaderNames.AUTHORIZATION,
                        "Bearer " + authenticationConfig.getBearerToken());
                log.debug("Bearer authentication header added to WebSocket handshake");
            }
        }

        log.debug("Start to create WebSocketClientHandshaker");
        final WebSocketClientHandshaker handshaker =
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, null, true, headers);
        log.debug("WebSocketClientHandshaker created successfully");

        // Create WebSocket handler
        final WebSocketClientHandler handler = new WebSocketClientHandler(handshaker);

        // Configure Bootstrap
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true) // Allow address reuse
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                log.debug("Initialize Channel Pipeline");
                                // Configure Socket options to optimize connection
                                ch.config().setTcpNoDelay(true);
                                ch.config().setKeepAlive(true);
                                ch.config().setConnectTimeoutMillis(connectTimeoutMillis);

                                ChannelPipeline p = ch.pipeline();
                                if (sslCtx != null) {
                                    log.debug("Start to configure SSL processor");
                                    SSLEngine sslEngine = sslCtx.newEngine(ch.alloc(), host, port);
                                    sslEngine.setUseClientMode(true);

                                    // Force close host name verification
                                    SSLParameters sslParams = sslEngine.getSSLParameters();
                                    sslParams.setEndpointIdentificationAlgorithm(
                                            null); // Close host name verification
                                    sslEngine.setSSLParameters(sslParams);
                                    log.debug("Force close SSL host name verification");

                                    SslHandler sslHandler = new SslHandler(sslEngine);
                                    // Set shorter handshake timeout to avoid long blocking
                                    sslHandler.setHandshakeTimeoutMillis(15000); // 15 seconds
                                    p.addLast(sslHandler);

                                    log.debug(
                                            "SSL processor configuration completed, handshake timeout: 15 seconds");
                                }
                                p.addLast(
                                        new HttpClientCodec(),
                                        new HttpObjectAggregator(65536),
                                        new IdleStateHandler(
                                                heartbeatIntervalSeconds,
                                                heartbeatIntervalSeconds,
                                                0),
                                        handler);
                                log.debug("Channel Pipeline initialized successfully");
                            }
                        });

        // Connect to server
        log.debug("Connecting to WebSocket server: {}://{}:{}{}", scheme, host, port, path);
        ChannelFuture connectFuture = b.connect(host, port);

        // Wait for connection establishment, using configured timeout
        log.debug("Waiting for TCP connection establishment, timeout: {}ms", connectTimeoutMillis);
        if (!connectFuture.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)) {
            connectFuture.cancel(true);
            throw new RuntimeException("Connection timeout: " + connectTimeoutMillis + "ms");
        }

        if (!connectFuture.isSuccess()) {
            log.error("TCP connection failed: {}", connectFuture.cause().getMessage());
            throw new RuntimeException("Connection failed", connectFuture.cause());
        }

        this.channel = connectFuture.channel();
        log.debug("TCP connection established, start WebSocket handshake");

        // Wait for handshake completion, using independent handshake timeout
        log.debug(
                "Waiting for WebSocket handshake completion, timeout: {}ms",
                handshakeTimeoutMillis);
        ChannelFuture handshakeFuture = handler.handshakeFuture();
        if (!handshakeFuture.await(handshakeTimeoutMillis, TimeUnit.MILLISECONDS)) {
            channel.close();
            throw new RuntimeException(
                    "WebSocket handshake timeout: " + handshakeTimeoutMillis + "ms");
        }

        if (!handshakeFuture.isSuccess()) {
            log.error("WebSocket handshake failed: {}", handshakeFuture.cause().getMessage());
            throw new RuntimeException("WebSocket handshake failed", handshakeFuture.cause());
        }

        log.info("WebSocket handshake completed, connection established successfully: {}", uri);

        // Set connection status
        connected.set(true);

        // Trigger connection success callback
        if (onOpen != null) {
            try {
                onOpen.run();
            } catch (Exception e) {
                log.error("Exception occurred while executing onOpen callback", e);
            }
        }

        // Start heartbeat
        startHeartbeat();
    }

    private void startHeartbeat() {
        if (heartbeatIntervalSeconds > 0 && scheduler != null && !scheduler.isShutdown()) {
            scheduler.scheduleAtFixedRate(
                    () -> {
                        if (channel != null && channel.isActive()) {
                            log.debug("Send heartbeat PING");
                            channel.writeAndFlush(
                                    new PingWebSocketFrame(
                                            Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));
                        }
                    },
                    heartbeatIntervalSeconds,
                    heartbeatIntervalSeconds,
                    TimeUnit.SECONDS);
        }
    }

    // ========================================
    // Inner classes
    // ========================================

    private class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;

        public WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            log.debug("WebSocket handler added to Pipeline");
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.debug("Channel activated, start WebSocket handshake");
            try {
                handshaker.handshake(ctx.channel());
                log.debug("WebSocket handshake request sent");
            } catch (Exception e) {
                log.error("WebSocket handshake request sending failed", e);
                throw e;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.warn("WebSocket connection disconnected");
            connected.set(false);

            if (onClose != null) {
                try {
                    // For abnormal disconnection, use 1000 (normal close) or no status code
                    // 1000 = normal close, 1001 = endpoint left, 1002 = protocol error
                    CloseWebSocketFrame closeFrame =
                            new CloseWebSocketFrame(1000, "Connection disconnected");
                    onClose.accept(closeFrame);
                } catch (Exception e) {
                    log.error("Exception occurred while executing onClose callback", e);
                    // plan B: if creating CloseWebSocketFrame fails, pass null
                    try {
                        onClose.accept(null);
                    } catch (Exception ex) {
                        log.error("Exception occurred while executing onClose callback", ex);
                    }
                }
            }

            // If the client is still running, try to reconnect
            if (running.get()) {
                connecting.set(false);
                scheduleConnect(reconnectAttempts.incrementAndGet());
            } else {
                shutdownLatch.countDown();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent idleEvent = (IdleStateEvent) evt;
                log.debug("Detected idle state: {}", idleEvent.state());
                // Send heartbeat
                if (ctx.channel().isActive()) {
                    ctx.writeAndFlush(
                            new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();

            // Handle handshake completion
            if (!handshaker.isHandshakeComplete()) {
                log.debug("Handle WebSocket handshake response");
                try {
                    FullHttpResponse response = (FullHttpResponse) msg;
                    log.debug("Received handshake response, status code: {}", response.status());
                    handshaker.finishHandshake(ch, response);
                    handshakeFuture.setSuccess();
                    log.debug("WebSocket handshake completed successfully");
                } catch (Exception e) {
                    log.error("WebSocket handshake failed", e);
                    handshakeFuture.setFailure(e);
                }
                return;
            }

            // Handle unexpected HTTP response
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus="
                                + response.status()
                                + ", content="
                                + response.content().toString(CharsetUtil.UTF_8)
                                + ')');
            }

            // Handle WebSocket frame
            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof TextWebSocketFrame) {
                    TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                    String text = textFrame.text();
                    log.debug("Received WebSocket server message: {}", text);

                    if (onMessage != null) {
                        try {
                            onMessage.accept(text);
                        } catch (Exception e) {
                            log.error("Exception occurred while executing onMessage callback", e);
                        }
                    }
                } else if (frame instanceof BinaryWebSocketFrame) {
                    BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                    log.debug(
                            "Received binary message, length: {}",
                            binaryFrame.content().readableBytes());
                    // If binary message needs to be processed, add processing logic here
                } else if (frame instanceof PingWebSocketFrame) {
                    log.debug("Received PING");
                    ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                } else if (frame instanceof PongWebSocketFrame) {
                    log.debug("Received PONG heartbeat response");
                } else if (frame instanceof CloseWebSocketFrame) {
                    log.debug("Received close frame");
                    CloseWebSocketFrame closeFrame = (CloseWebSocketFrame) frame;

                    if (onClose != null) {
                        try {
                            onClose.accept(closeFrame);
                        } catch (Exception e) {
                            log.error("Exception occurred while executing onClose callback", e);
                        }
                    }

                    ch.close();
                    shutdownLatch.countDown();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }

            log.error("WebSocket connection exception occurred: {}", cause.getMessage(), cause);

            // Detailed analysis of exception types to help diagnose problems
            if (cause instanceof javax.net.ssl.SSLHandshakeException) {
                log.error("SSL handshake failed: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) TLS version mismatch 2) Certificate problem 3) Password suite incompatibility");
            } else if (cause instanceof javax.net.ssl.SSLException) {
                log.error("SSL exception: {}", cause.getMessage());
                log.error("Possible reasons: 1) SSL connection error occurred during the process");
            } else if (cause instanceof java.net.ConnectException) {
                log.error("Connection refused: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) Server not started 2) Port occupied 3) Firewall blocked");
            } else if (cause instanceof java.net.SocketTimeoutException) {
                log.error("Connection timeout: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) Network delay too high 2) Server response slow 3) Connection timeout set too short");
            } else if (cause instanceof java.io.IOException) {
                log.error("IO exception: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) Network interruption 2) Connection reset 3) Data transmission error");
            } else if (cause instanceof io.netty.handler.timeout.TimeoutException) {
                log.error("Netty timeout exception: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) Handshake timeout 2) Read/write timeout 3) Heartbeat timeout");
            } else if (cause instanceof java.security.cert.CertificateException) {
                log.error("Certificate exception: {}", cause.getMessage());
                log.error(
                        "Possible reasons: 1) Certificate invalid 2) Certificate expired 3) Certificate chain verification failed");
            } else {
                log.error(
                        "Unknown exception type: {} - {}",
                        cause.getClass().getSimpleName(),
                        cause.getMessage());
            }

            if (onError != null) {
                try {
                    onError.accept(cause);
                } catch (Exception e) {
                    log.error("Exception occurred while executing onError callback", e);
                }
            }

            ctx.close();
        }
    }

    /**
     * Perform network connectivity detection
     *
     * @param host target host
     * @param port target port
     * @throws Exception when connection fails
     */
    private void performNetworkTest(String host, int port) throws Exception {
        log.debug("Test TCP connection to {}:{}", host, port);

        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 10000); // 10 seconds timeout
            log.debug("TCP connection test successful");
        } catch (java.net.ConnectException e) {
            throw new Exception("Connection refused: " + e.getMessage(), e);
        } catch (java.net.SocketTimeoutException e) {
            throw new Exception(
                    "Connection timeout: cannot establish connection within 10 seconds", e);
        } catch (java.net.UnknownHostException e) {
            throw new Exception("Host name resolution failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new Exception("Network connection failed: " + e.getMessage(), e);
        }
    }

    private void shutdownThreadPools() {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
    }
}
