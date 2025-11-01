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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * PI Configuration Helper
 *
 * <p>Provides convenient methods to access PI configuration values from ReadonlyConfig. This
 * replaces the need for PIConnectionConfig class.
 */
public class PIConfigHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter[] SUPPORTED_FORMATTERS = {
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    private final ReadonlyConfig config;

    public PIConfigHelper(ReadonlyConfig config) {
        this.config = config;
    }

    // ================= Core Configuration =================

    public String getServerUrl() {
        return config.get(PIConfig.PI_WEB_API_URL);
    }

    public List<String> getPiPaths() {
        return config.getOptional(PIConfig.PI_PATHS).orElse(null);
    }

    // ================= Authentication Configuration =================

    public AuthType getAuthType() {
        return config.get(PIConfig.AUTH_TYPE);
    }

    public String getUsername() {
        return config.getOptional(PIConfig.USERNAME).orElse(null);
    }

    public String getPassword() {
        return config.getOptional(PIConfig.PASSWORD).orElse(null);
    }

    public String getBearerToken() {
        return config.getOptional(PIConfig.BEARER_TOKEN).orElse(null);
    }

    // ================= Connection Configuration =================

    public int getConnectionTimeoutMs() {
        return config.getOptional(PIConfig.CONNECTION_TIMEOUT_MS).orElse(30000);
    }

    public int getReadTimeoutMs() {
        return config.getOptional(PIConfig.READ_TIMEOUT_MS).orElse(40000);
    }

    public int getRetryAttempts() {
        return config.getOptional(PIConfig.RETRY_ATTEMPTS).orElse(3);
    }

    public long getRetryBackoffMultiplierMs() {
        return config.getOptional(PIConfig.RETRY_BACKOFF_MULTIPLIER_MS).orElse(1000L);
    }

    public long getRetryBackoffMaxMs() {
        return config.getOptional(PIConfig.RETRY_BACKOFF_MAX_MS).orElse(10000L);
    }

    public int getWebSocketMaxRetries() {
        return config.getOptional(PIConfig.WEBSOCKET_MAX_RETRIES).orElse(5);
    }

    public boolean isTrustAllCerts() {
        return config.getOptional(PIConfig.TRUST_ALL_CERTS).orElse(true);
    }

    public boolean isVerifyHostname() {
        return config.getOptional(PIConfig.VERIFY_HOSTNAME).orElse(false);
    }

    // ================= Batch Configuration =================

    public String getStartTime() {
        return config.getOptional(PIConfig.START_TIME).orElse(null);
    }

    public String getEndTime() {
        return config.getOptional(PIConfig.END_TIME).orElse(null);
    }

    public int getMaxCount() {
        return config.getOptional(PIConfig.MAX_COUNT).orElse(100000);
    }

    public String getBoundaryType() {
        return config.getOptional(PIConfig.BOUNDARY_TYPE).orElse("Inside");
    }

    public int getBatchWindowMinutes() {
        return config.getOptional(PIConfig.BATCH_WINDOW_MINUTES).orElse(3);
    }

    // ================= JSON Configuration =================

    public Map<String, String> getJsonField() {
        return config.getOptional(PIConfig.JSON_FIELD).orElse(null);
    }

    // ================= WebSocket Configuration =================

    public boolean isIncludeInitialValues() {
        return config.getOptional(PIConfig.INCLUDE_INITIAL_VALUES).orElse(true);
    }

    public int getHeartbeatRate() {
        return config.getOptional(PIConfig.HEARTBEAT_RATE).orElse(10);
    }

    public String getRetrievalMode() {
        return config.getOptional(PIConfig.RETRIEVAL_MODE).orElse("Auto");
    }

    // ================= Utility Methods =================

    public boolean isHttps() {
        return getServerUrl().toLowerCase().startsWith("https");
    }

    /** Parse timestamp string to LocalDateTime */
    public LocalDateTime parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            return null;
        }

        String cleanedStr = timestampStr.trim();

        for (DateTimeFormatter formatter : SUPPORTED_FORMATTERS) {
            try {
                return LocalDateTime.parse(cleanedStr, formatter);
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }

        throw new IllegalArgumentException("Unable to parse timestamp: " + timestampStr);
    }

    /** Get parsed start time as LocalDateTime */
    public LocalDateTime getParsedStartTime() {
        return parseTimestamp(getStartTime());
    }

    /** Get parsed end time as LocalDateTime */
    public LocalDateTime getParsedEndTime() {
        return parseTimestamp(getEndTime());
    }

    /** Get start time as LocalDateTime (alias for getParsedStartTime) */
    public LocalDateTime getStartDateTime() {
        return getParsedStartTime();
    }

    /** Get end time as LocalDateTime (alias for getParsedEndTime) */
    public LocalDateTime getEndDateTime() {
        return getParsedEndTime();
    }

    public int getWebIdsPerSplit() {
        return config.getOptional(PIConfig.WEBIDS_PER_SPLIT).orElse(20);
    }

    public int getMaxSplits() {
        return config.getOptional(PIConfig.MAX_SPLITS).orElse(100);
    }

    public boolean getAutoAdjustSplitSize() {
        return config.getOptional(PIConfig.AUTO_ADJUST_SPLIT_SIZE).orElse(true);
    }

    public int getWebIdResolveBatchSize() {
        return config.getOptional(PIConfig.WEBID_RESOLVE_BATCH_SIZE).orElse(50);
    }

    public long getWebIdResolveDelayMs() {
        return config.getOptional(PIConfig.WEBID_RESOLVE_DELAY_MS).orElse(10L);
    }

    public int getWebSocketConnectionWaitTimeoutMs() {
        return config.getOptional(PIConfig.WEBSOCKET_CONNECTION_WAIT_TIMEOUT_MS).orElse(120000);
    }

    public int getChannelPollingIntervalMs() {
        return config.getOptional(PIConfig.CHANNEL_POLLING_INTERVAL_MS).orElse(3000);
    }

    public MetadataType getMetadataType() {
        return config.getOptional(PIConfig.METADATA_TYPE).orElse(null);
    }

    public int getMaxWebIDsPerSplit() {
        return config.get(PIConfig.MAX_WEBIDS_PER_SPLIT);
    }

    public int getDataBufferQueueSize() {
        return config.get(PIConfig.DATA_BUFFER_QUEUE_SIZE);
    }

    public int getBatchDrainSize() {
        return config.get(PIConfig.BATCH_DRAIN_SIZE);
    }

    public int getBufferLowThreshold() {
        return config.get(PIConfig.BUFFER_LOW_THRESHOLD);
    }
}
