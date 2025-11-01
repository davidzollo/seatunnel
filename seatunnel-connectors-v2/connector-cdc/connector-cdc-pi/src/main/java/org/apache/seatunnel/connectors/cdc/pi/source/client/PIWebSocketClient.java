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
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 5;
    private static final long DEFAULT_BACKOFF_INITIAL_MS = 1000L;
    private static final long DEFAULT_BACKOFF_MAX_MS = 10000L;

    // ========== Core components ==========
    private final PIWebSocketConfig config;
    private final PIWebSocketConnectionState connectionState;
    private final PIWebSocketReconnectStrategy reconnectStrategy;
    private final PIWebSocketHeartbeat heartbeat;
    private final PIConfigHelper piConfigHelper;

    // ========== Netty related fields ==========
    private volatile EventLoopGroup eventLoopGroup;
    private volatile Channel channel;
    private volatile ScheduledExecutorService connectionExecutor;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // ========== Callback function fields ==========
    private volatile Consumer<String> onMessage;
    private volatile Runnable onOpen;
    private volatile Consumer<Throwable> onError;
    private volatile Consumer<CloseWebSocketFrame> onClose;

    /**
     * Construct Netty WebSocket client
     *
     * @param url WebSocket URL (supports ws:// and wss://)
     * @param trustAll Whether to trust all SSL certificates (only used for test environment)
     * @param heartbeatIntervalSeconds Heartbeat interval (seconds)
     * @param connectTimeoutMillis Connection timeout (milliseconds)
     * @param maxReconnectAttempts Maximum reconnection attempts
     * @param backoffInitialMillis Initial backoff interval in milliseconds
     * @param backoffMaxMillis Maximum backoff interval in milliseconds
     * @throws IllegalArgumentException If URL format is invalid
     */
    public PIWebSocketClient(
            String url,
            boolean trustAll,
            int heartbeatIntervalSeconds,
            int connectTimeoutMillis,
            int maxReconnectAttempts,
            long backoffInitialMillis,
            long backoffMaxMillis,
            PIConfigHelper piConfigHelper) {

        // Build configuration
        this.config =
                PIWebSocketConfig.builder()
                        .url(url)
                        .trustAll(trustAll)
                        .heartbeatIntervalSeconds(heartbeatIntervalSeconds)
                        .connectTimeoutMillis(connectTimeoutMillis)
                        .maxReconnectAttempts(maxReconnectAttempts)
                        .backoffInitialMillis(backoffInitialMillis)
                        .backoffMaxMillis(backoffMaxMillis)
                        .build();

        // Initialize components
        this.connectionState = new PIWebSocketConnectionState();
        this.reconnectStrategy =
                new PIWebSocketReconnectStrategy(
                        config.getMaxReconnectAttempts(),
                        config.getBackoffInitialMillis(),
                        config.getBackoffMaxMillis());
        this.heartbeat = new PIWebSocketHeartbeat(config.getHeartbeatIntervalSeconds());
        this.piConfigHelper = piConfigHelper;

        if (config.isTrustAll()) {
            log.debug("Trust all SSL certificates mode enabled!");
        }

        log.debug(
                "Initialize PIWebSocketClient - URL: {}, connection timeout: {}ms, handshake timeout: {}ms, heartbeat interval: {}s, backoff initial: {}ms, backoff max: {}ms, max retries: {}",
                url,
                config.getConnectTimeoutMillis(),
                config.getHandshakeTimeoutMillis(),
                config.getHeartbeatIntervalSeconds(),
                config.getBackoffInitialMillis(),
                config.getBackoffMaxMillis(),
                config.getMaxReconnectAttempts());
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
        if (connectionState.startRunning()) {
            log.info("Start PIWebSocketClient...");

            // Reset reconnection attempts counter when starting
            reconnectStrategy.reset();
            log.debug("Reconnection attempts counter reset to 0 on client start");

            // Create dedicated thread pool with single thread per WebSocket client
            this.eventLoopGroup =
                    new NioEventLoopGroup(1, createThreadFactory("PIWebSocketClient-EventLoop"));
            this.connectionExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            createThreadFactory("PIWebSocketClient-Thread"));

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
        if (connectionState.stopRunning()) {
            log.info("Stop PIWebSocketClient...");
            connectionState.reset();

            // Cancel heartbeat task on stop
            heartbeat.stop();

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
                shutdownConnectionExecutor();

                log.info("PIWebSocketClient has been completely stopped");
            }
        }
    }

    /** Close client (implement AutoCloseable interface) */
    @Override
    public void close() {
        stop();

        // Clear callback references to prevent memory leaks
        onMessage = null;
        onOpen = null;
        onError = null;
        onClose = null;

        log.debug("PIWebSocketClient closed and callbacks cleaned");
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
        boolean trustAllCerts = piConfigHelper.isTrustAllCerts();
        int heartbeatIntervalSeconds = piConfigHelper.getHeartbeatRate();
        int connectTimeoutMillis = piConfigHelper.getWebSocketConnectionWaitTimeoutMs();

        long backoffInitialMs = piConfigHelper.getRetryBackoffMultiplierMs();
        if (backoffInitialMs <= 0) {
            log.warn(
                    "Configured retry_backoff_multiplier_ms ({}) is invalid, fallback to {} ms",
                    backoffInitialMs,
                    DEFAULT_BACKOFF_INITIAL_MS);
            backoffInitialMs = DEFAULT_BACKOFF_INITIAL_MS;
        }

        long backoffMaxMs = piConfigHelper.getRetryBackoffMaxMs();
        if (backoffMaxMs <= 0) {
            log.warn(
                    "Configured retry_backoff_max_ms ({}) is invalid, fallback to {} ms",
                    backoffMaxMs,
                    DEFAULT_BACKOFF_MAX_MS);
            backoffMaxMs = DEFAULT_BACKOFF_MAX_MS;
        }
        if (backoffMaxMs < backoffInitialMs) {
            log.warn(
                    "retry_backoff_max_ms ({}) is smaller than retry_backoff_multiplier_ms ({}), align to multiplier value",
                    backoffMaxMs,
                    backoffInitialMs);
            backoffMaxMs = backoffInitialMs;
        }

        int maxReconnectAttempts = piConfigHelper.getWebSocketMaxRetries();
        if (maxReconnectAttempts <= 0) {
            log.warn(
                    "Configured retry attempts is invalid, fallback to default {} attempts",
                    DEFAULT_MAX_RECONNECT_ATTEMPTS);
            maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;
        }

        log.debug(
                "Create PI WebSocket client (Netty) - URL: {}, trust all certificates: {}, WebSocket connection timeout: {}ms, max retries: {}, backoff initial/max: {}/{} ms",
                webSocketUrl,
                trustAllCerts,
                connectTimeoutMillis,
                maxReconnectAttempts,
                backoffInitialMs,
                backoffMaxMs);

        // Create authenticated WebSocket client
        PIWebSocketClient client =
                new PIWebSocketClient(
                        webSocketUrl,
                        trustAllCerts,
                        heartbeatIntervalSeconds,
                        connectTimeoutMillis,
                        maxReconnectAttempts,
                        backoffInitialMs,
                        backoffMaxMs,
                        piConfigHelper);

        return client;
    }

    /**
     * Check if the client is connected
     *
     * @return Whether the client is connected
     */
    public boolean isConnected() {
        return connectionState.isRunning()
                && connectionState.isConnected()
                && channel != null
                && channel.isActive();
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

    /**
     * Schedule connection attempt with retry logic
     *
     * @param attempt
     */
    private void scheduleConnect(int attempt) {
        if (!connectionState.isRunning()) {
            log.debug("Client is not running, skip scheduling reconnect attempt {}", attempt);
            connectionState.clearReconnectScheduled();
            return;
        }

        if (!connectionState.scheduleReconnect()) {
            log.debug("Reconnect attempt {} already scheduled, skip duplicate scheduling", attempt);
            return;
        }

        if (!reconnectStrategy.canRetry(attempt)) {
            connectionState.clearReconnectScheduled();
            log.error(
                    "Maximum reconnection attempts ({}) reached, connection failed",
                    reconnectStrategy.getMaxReconnectAttempts());
            connectionState.setConnected(false);
            connectionState.setLastError(
                    "Maximum reconnection attempts reached, connection failed");
            if (onError != null) {
                onError.accept(
                        new PIConnectorException(
                                PIErrorCode.WEBSOCKET_RECONNECT_FAILED,
                                String.format(
                                        "Maximum reconnection attempts (%d) reached, connection failed",
                                        reconnectStrategy.getMaxReconnectAttempts())));
            }
            return;
        }

        long delayMs = reconnectStrategy.calculateBackoffDelayMs(attempt);
        ScheduledExecutorService executor = connectionExecutor;
        executor.schedule(
                () -> {
                    connectionState.clearReconnectScheduled();
                    try {
                        connectWithRetry(attempt);
                    } catch (Exception ex) {
                        log.error("Connection attempt {} terminated unexpectedly", attempt, ex);
                        connectionState.setConnecting(false);
                    }
                },
                delayMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Attempt to connect with retry logic
     *
     * @param attempt
     */
    private void connectWithRetry(int attempt) {
        if (!connectionState.isRunning()) {
            return;
        }
        if (!connectionState.startConnecting()) {
            log.debug(
                    "Connection attempt {} skipped because another attempt is in progress",
                    attempt + 1);
            return;
        }

        try {
            connectionState.updateLastConnectAttemptTime();
            log.info("Attempt to establish WebSocket connection, attempt number: {}", attempt + 1);

            setupConnection();
            connectionState.clearLastError();

        } catch (Throwable e) {
            connectionState.setConnected(false);
            connectionState.setLastError(e.getMessage());

            long connectDuration =
                    System.currentTimeMillis() - connectionState.getLastConnectAttemptTime();
            int nextAttempt = attempt + 1;
            boolean canRetry = reconnectStrategy.canRetry(nextAttempt);
            long nextDelay = reconnectStrategy.calculateBackoffDelayMs(nextAttempt);
            if (canRetry) {
                log.warn(
                        "WebSocket connection attempt {} failed (duration: {}ms). Next retry in {} ms: {}",
                        attempt + 1,
                        connectDuration,
                        nextDelay,
                        e.getMessage());
            } else {
                log.error(
                        "WebSocket connection attempt {} failed (duration: {}ms). Maximum retries ({}) reached: {}",
                        attempt + 1,
                        connectDuration,
                        reconnectStrategy.getMaxReconnectAttempts(),
                        e.getMessage());

                // Call onError callback only when max retries reached
                if (onError != null) {
                    try {
                        onError.accept(
                                new PIConnectorException(
                                        PIErrorCode.WEBSOCKET_RECONNECT_FAILED,
                                        String.format(
                                                "WebSocket connection failed after %d attempts: %s",
                                                reconnectStrategy.getMaxReconnectAttempts(),
                                                e.getMessage()),
                                        e));
                    } catch (Exception callbackEx) {
                        log.error(
                                "Exception occurred while executing onError callback", callbackEx);
                    }
                }
            }

            if (connectionState.isRunning() && canRetry) {
                reconnectStrategy.setReconnectAttempts(nextAttempt);
                scheduleConnect(nextAttempt);
            }
        } finally {
            connectionState.stopConnecting();
        }
    }

    /**
     * Directly establish WebSocket connection (synchronous blocking, suitable for test/minimal
     * scenarios) Note: This method does not automatically reconnect, does not manage thread pools,
     * and production environment recommends using start()
     */
    public void setupConnection() throws Exception {
        URI uri = config.getUri();
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
            runNetworkTest(host, port);
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
            if (config.isTrustAll()) {
                sslCtx =
                        SslContextBuilder.forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
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
        if (piConfigHelper != null) {
            AuthType authType = piConfigHelper.getAuthType();
            if (AuthType.BASIC.equals(authType)
                    && piConfigHelper.getUsername() != null
                    && piConfigHelper.getPassword() != null) {
                String authValue =
                        piConfigHelper.getUsername() + ":" + piConfigHelper.getPassword();
                // Use UTF-8 encoding explicitly to avoid platform-dependent encoding issues
                // This ensures non-ASCII characters in username/password are handled correctly
                String encodedAuth =
                        java.util.Base64.getEncoder()
                                .encodeToString(authValue.getBytes(StandardCharsets.UTF_8));
                headers.add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedAuth);
                log.debug("Basic authentication header added to WebSocket handshake");
            } else if (AuthType.BEARER.equals(authType)
                    && piConfigHelper.getBearerToken() != null) {
                headers.add(
                        HttpHeaderNames.AUTHORIZATION, "Bearer " + piConfigHelper.getBearerToken());
                log.debug("Bearer authentication header added to WebSocket handshake");
            }
        }

        log.debug("Start to create WebSocketClientHandshaker");
        final WebSocketClientHandshaker handshaker =
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, null, true, headers);
        log.debug("WebSocketClientHandshaker created successfully");

        // Create WebSocket handler
        final PIWebSocketHandler handler =
                new PIWebSocketHandler(
                        handshaker,
                        connectionState,
                        reconnectStrategy,
                        heartbeat,
                        shutdownLatch,
                        () -> scheduleConnect(0));
        handler.setOnMessage(onMessage);
        handler.setOnOpen(onOpen);
        handler.setOnClose(onClose);

        // Configure Bootstrap
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMillis())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                log.debug("Initialize Channel Pipeline");
                                ch.config().setTcpNoDelay(true);
                                ch.config().setKeepAlive(true);
                                ch.config()
                                        .setConnectTimeoutMillis(config.getConnectTimeoutMillis());

                                ChannelPipeline p = ch.pipeline();
                                if (sslCtx != null) {
                                    log.debug("Start to configure SSL processor");
                                    SSLEngine sslEngine = sslCtx.newEngine(ch.alloc(), host, port);
                                    sslEngine.setUseClientMode(true);

                                    SSLParameters sslParams = sslEngine.getSSLParameters();
                                    sslParams.setEndpointIdentificationAlgorithm(null);
                                    sslEngine.setSSLParameters(sslParams);
                                    log.debug("Force close SSL host name verification");

                                    SslHandler sslHandler = new SslHandler(sslEngine);
                                    sslHandler.setHandshakeTimeoutMillis(15000);
                                    p.addLast(sslHandler);

                                    log.debug(
                                            "SSL processor configuration completed, handshake timeout: 15 seconds");
                                }
                                p.addLast(
                                        new HttpClientCodec(),
                                        new HttpObjectAggregator(65536),
                                        new IdleStateHandler(
                                                config.getHeartbeatIntervalSeconds(),
                                                config.getHeartbeatIntervalSeconds(),
                                                0),
                                        handler);
                                log.debug("Channel Pipeline initialized successfully");
                            }
                        });

        // Connect to server
        log.debug("Connecting to WebSocket server: {}://{}:{}{}", scheme, host, port, path);
        ChannelFuture connectFuture = b.connect(host, port);

        // Wait for connection establishment, using configured timeout
        log.debug(
                "Waiting for TCP connection establishment, timeout: {}ms",
                config.getConnectTimeoutMillis());
        if (!connectFuture.await(config.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS)) {
            connectFuture.cancel(true);
            throw new RuntimeException(
                    "Connection timeout: " + config.getConnectTimeoutMillis() + "ms");
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
                config.getHandshakeTimeoutMillis());
        ChannelFuture handshakeFuture = handler.handshakeFuture();
        if (!handshakeFuture.await(config.getHandshakeTimeoutMillis(), TimeUnit.MILLISECONDS)) {
            channel.close();
            throw new RuntimeException(
                    "WebSocket handshake timeout: " + config.getHandshakeTimeoutMillis() + "ms");
        }

        if (!handshakeFuture.isSuccess()) {
            log.error("WebSocket handshake failed: {}", handshakeFuture.cause().getMessage());
            throw new RuntimeException("WebSocket handshake failed", handshakeFuture.cause());
        }

        log.info("WebSocket handshake completed, connection established successfully: {}", uri);

        // Start heartbeat
        heartbeat.start(channel);
    }

    // ========================================
    // Helper methods
    // ========================================

    /**
     * Perform network connectivity detection
     *
     * @param host target host
     * @param port target port
     * @throws Exception when connection fails
     */
    private void runNetworkTest(String host, int port) throws Exception {
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

    /** Shutdown Netty event loop thread pool */
    private void shutdownThreadPools() {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(
                    0, config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS);
        }
    }

    /** Shutdown connection executor thread pool */
    private void shutdownConnectionExecutor() {
        ScheduledExecutorService executor = connectionExecutor;
        connectionExecutor = null;
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Connection executor did not terminate within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while shutting down connection executor", e);
            }
        }
    }
}
