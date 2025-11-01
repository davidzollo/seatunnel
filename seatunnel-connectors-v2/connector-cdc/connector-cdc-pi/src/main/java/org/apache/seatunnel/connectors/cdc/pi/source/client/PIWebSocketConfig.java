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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configuration for PIWebSocketClient
 *
 * <p>Encapsulates all connection configuration parameters with validation
 */
public class PIWebSocketConfig {

    private static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 5;
    private static final long DEFAULT_BACKOFF_INITIAL_MS = 1000L;
    private static final long DEFAULT_BACKOFF_MAX_MS = 10000L;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final URI uri;
    private final boolean trustAll;
    private final int heartbeatIntervalSeconds;
    private final int connectTimeoutMillis;
    private final int handshakeTimeoutMillis;
    private final int maxReconnectAttempts;
    private final long backoffInitialMillis;
    private final long backoffMaxMillis;

    private PIWebSocketConfig(Builder builder) {
        this.uri = builder.uri;
        this.trustAll = builder.trustAll;
        this.heartbeatIntervalSeconds = builder.heartbeatIntervalSeconds;
        this.connectTimeoutMillis = builder.connectTimeoutMillis;
        this.handshakeTimeoutMillis = builder.handshakeTimeoutMillis;
        this.maxReconnectAttempts = builder.maxReconnectAttempts;
        this.backoffInitialMillis = builder.backoffInitialMillis;
        this.backoffMaxMillis = builder.backoffMaxMillis;
    }

    public URI getUri() {
        return uri;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public long getBackoffInitialMillis() {
        return backoffInitialMillis;
    }

    public long getBackoffMaxMillis() {
        return backoffMaxMillis;
    }

    public int getShutdownTimeoutSeconds() {
        return SHUTDOWN_TIMEOUT_SECONDS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private URI uri;
        private boolean trustAll;
        private int heartbeatIntervalSeconds;
        private int connectTimeoutMillis;
        private int handshakeTimeoutMillis;
        private int maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;
        private long backoffInitialMillis = DEFAULT_BACKOFF_INITIAL_MS;
        private long backoffMaxMillis = DEFAULT_BACKOFF_MAX_MS;

        public Builder url(String url) {
            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("WebSocket URL cannot be empty");
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

            return this;
        }

        public Builder trustAll(boolean trustAll) {
            this.trustAll = trustAll;
            return this;
        }

        public Builder heartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
            if (heartbeatIntervalSeconds < 0) {
                throw new IllegalArgumentException(
                        "Heartbeat interval cannot be less than 0 seconds");
            }
            this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
            return this;
        }

        public Builder connectTimeoutMillis(int connectTimeoutMillis) {
            if (connectTimeoutMillis < 1000) {
                throw new IllegalArgumentException(
                        "Connection timeout must be greater than or equal to 1000 milliseconds");
            }
            this.connectTimeoutMillis = connectTimeoutMillis;
            this.handshakeTimeoutMillis = Math.max(connectTimeoutMillis * 2, 30000);
            return this;
        }

        public Builder maxReconnectAttempts(int maxReconnectAttempts) {
            if (maxReconnectAttempts < 0) {
                throw new IllegalArgumentException("Max reconnect attempts cannot be negative");
            }
            this.maxReconnectAttempts =
                    maxReconnectAttempts > 0
                            ? maxReconnectAttempts
                            : DEFAULT_MAX_RECONNECT_ATTEMPTS;
            return this;
        }

        public Builder backoffInitialMillis(long backoffInitialMillis) {
            if (backoffInitialMillis < 0) {
                throw new IllegalArgumentException("Backoff interval must be non-negative");
            }
            this.backoffInitialMillis =
                    backoffInitialMillis > 0 ? backoffInitialMillis : DEFAULT_BACKOFF_INITIAL_MS;
            return this;
        }

        public Builder backoffMaxMillis(long backoffMaxMillis) {
            if (backoffMaxMillis < 0) {
                throw new IllegalArgumentException("Max backoff interval must be non-negative");
            }
            long resolvedBackoffMax =
                    backoffMaxMillis > 0 ? backoffMaxMillis : DEFAULT_BACKOFF_MAX_MS;
            this.backoffMaxMillis = Math.max(this.backoffInitialMillis, resolvedBackoffMax);
            return this;
        }

        public PIWebSocketConfig build() {
            if (uri == null) {
                throw new IllegalStateException("URL must be set");
            }
            if (connectTimeoutMillis <= 0) {
                throw new IllegalStateException("Connect timeout must be set");
            }
            return new PIWebSocketConfig(this);
        }
    }
}
