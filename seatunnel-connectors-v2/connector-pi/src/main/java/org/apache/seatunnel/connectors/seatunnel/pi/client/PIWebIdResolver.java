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

package org.apache.seatunnel.connectors.seatunnel.pi.client;

import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.MetadataType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PI WebID Resolver
 *
 * <p>Responsible for automatically resolving PI Path to WebID, supporting batch resolution and
 * caching mechanism Supports both AF Attribute and PI Point path resolution
 */
@Slf4j
public class PIWebIdResolver {
    private final PIConfigHelper config;
    private final ObjectMapper objectMapper;
    private final Map<String, String> webIdCache;
    private final PIHttpClient httpClient;

    public PIWebIdResolver(PIConfigHelper config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.webIdCache = new ConcurrentHashMap<>();
        this.httpClient = null;

        // Configure SSL to trust all certificates (production environment should use proper
        // certificate verification)
        configureTrustAllSSL();
    }

    /** Create WebID resolver using existing HTTP client */
    public PIWebIdResolver(PIConfigHelper config, PIHttpClient httpClient) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.webIdCache = new ConcurrentHashMap<>();
        this.httpClient = httpClient;
    }

    /**
     * Check if the PI Path is an AF Attribute path based on configured metadata type
     *
     * @param piPath PI Path to check
     * @return true if it's an AF Attribute path, false if it's a PI Point path
     */
    private boolean isAttributePath(String piPath) {
        MetadataType metadataType = config.getMetadataType();
        if (metadataType == null) {
            throw new IllegalStateException("metadata_type must be configured");
        }
        return metadataType == MetadataType.ATTRIBUTES;
    }

    /**
     * Build the correct endpoint URL based on PI Path type
     *
     * @param serverUrl Base server URL
     * @param piPath PI Path to resolve
     * @return Complete endpoint URL
     */
    private String buildEndpointUrl(String serverUrl, String piPath) {
        // Check if serverUrl already contains /points or /attributes
        if (serverUrl.contains("/points") || serverUrl.contains("/attributes")) {
            return serverUrl;
        }

        // Auto-detect endpoint based on PI Path type
        if (isAttributePath(piPath)) {
            return serverUrl + "/attributes";
        } else {
            return serverUrl + "/points";
        }
    }

    /** Batch resolve PI Paths to WebIDs, returns Map<PI Path, WebID> */
    public Map<String, String> resolveWebIdsAsMap(List<String> piPaths) throws Exception {
        if (piPaths == null || piPaths.isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_MISSING_TAG_PATHS, "PI Paths cannot be empty");
        }

        Map<String, String> resolvedMap = new HashMap<>();
        List<String> unresolvedPaths = new ArrayList<>();

        // First check cache
        for (String piPath : piPaths) {
            String cachedWebId = webIdCache.get(piPath);
            if (cachedWebId != null) {
                resolvedMap.put(piPath, cachedWebId);
                log.debug("Retrieved WebID from cache: {} -> {}", piPath, cachedWebId);
            } else {
                unresolvedPaths.add(piPath);
            }
        }

        // Batch resolve uncached Tag Paths
        if (!unresolvedPaths.isEmpty()) {
            Map<String, String> newlyResolved = batchResolveWebIds(unresolvedPaths);

            // Update cache and result mapping
            for (Map.Entry<String, String> entry : newlyResolved.entrySet()) {
                String piPath = entry.getKey();
                String webId = entry.getValue();

                webIdCache.put(piPath, webId);
                resolvedMap.put(piPath, webId);

                log.debug("Resolved PI Path: {} -> WebID: {}", piPath, webId);
            }
        }

        log.info("Successfully resolved {} PI Paths to WebIDs", resolvedMap.size());
        return resolvedMap;
    }

    /** Resolve single PI Path to WebID, used for PICDCSourceReader */
    public String resolveWebId(String piPath) {
        if (piPath == null || piPath.trim().isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.PI_TAG_PATH_INVALID, "PI Path cannot be empty");
        }

        // Check cache
        String cachedWebId = webIdCache.get(piPath);
        if (cachedWebId != null) {
            return cachedWebId;
        }

        try {
            // Build PI Web API request URL
            String encodedPath;
            try {
                encodedPath = URLEncoder.encode(piPath, StandardCharsets.UTF_8.name());
            } catch (java.io.UnsupportedEncodingException e) {
                // UTF-8 is always supported, this should never happen
                throw new RuntimeException("UTF-8 encoding not supported", e);
            }

            // Build correct endpoint URL based on PI Path type
            String endpointUrl = buildEndpointUrl(config.getServerUrl(), piPath);
            String requestUrl = endpointUrl + "?path=" + encodedPath;

            // Right url:
            // https://10.89.63.4:8443/piwebapi/points?path=%5C%5Cpims.huafeng.com%5CHF.AA.NAB%3ALIA-26101.PV
            log.debug("Preparing to resolve PI Path: {} -> URL: {}", piPath, requestUrl);

            // Send HTTP request
            HttpsURLConnection connection = createConnection(requestUrl);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");

            // Add authentication header
            addAuthenticationHeader(connection);

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                String responseBody = readResponse(connection);
                JsonNode responseJson = objectMapper.readTree(responseBody);

                String webId = responseJson.path("WebId").asText();
                if (webId != null && !webId.trim().isEmpty()) {
                    // Cache result
                    webIdCache.put(piPath, webId);
                    log.info("Successfully resolved PI Path: {} -> WebID: {}", piPath, webId);
                    return webId;
                } else {
                    throw new PIConnectorException(
                            PIErrorCode.WEBID_NOT_FOUND,
                            "PI Web API returned empty WebID: " + piPath);
                }
            } else {
                String errorResponse = readErrorResponse(connection);
                throw new PIConnectorException(
                        PIErrorCode.WEBID_RESOLUTION_FAILED,
                        String.format(
                                "Failed to resolve PI Path: %s, HTTP status code: %d, response: %s",
                                piPath, responseCode, errorResponse));
            }

        } catch (IOException e) {
            throw new PIConnectorException(
                    PIErrorCode.WEBID_RESOLUTION_FAILED,
                    "Network exception occurred while resolving PI Path: " + piPath,
                    e);
        } catch (PIConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new PIConnectorException(
                    PIErrorCode.WEBID_RESOLUTION_FAILED,
                    "Exception occurred while resolving PI Path: " + piPath,
                    e);
        }
    }

    /** Batch resolve WebIDs (concurrent processing for improved efficiency) */
    private Map<String, String> batchResolveWebIds(List<String> piPaths) {
        Map<String, String> results = new ConcurrentHashMap<>();
        List<Exception> failures = new ArrayList<>();

        // Use parallel stream to improve resolution efficiency
        piPaths.parallelStream()
                .forEach(
                        piPath -> {
                            try {
                                String webId = resolveSingleWebId(piPath);
                                if (webId != null && !webId.trim().isEmpty()) {
                                    results.put(piPath, webId);
                                    log.info(
                                            "Successfully resolved PI Path: {} -> WebID: {}",
                                            piPath,
                                            webId);
                                } else {
                                    failures.add(
                                            new PIConnectorException(
                                                    PIErrorCode.WEBID_NOT_FOUND,
                                                    "PI Web API returned empty WebID: " + piPath));
                                }
                            } catch (Exception e) {
                                log.error(
                                        "Failed to resolve PI Path: {}, error: {}",
                                        piPath,
                                        e.getMessage());
                                failures.add(e);
                            }
                        });

        // If any resolution fails, throw exception immediately
        if (!failures.isEmpty()) {
            Exception firstFailure = failures.get(0);
            throw new PIConnectorException(
                    PIErrorCode.WEBID_RESOLUTION_FAILED,
                    String.format(
                            "WebID batch resolution failed, failure count: %d/%d, first error: %s",
                            failures.size(), piPaths.size(), firstFailure.getMessage()),
                    firstFailure);
        }

        return results;
    }

    /**
     * Single WebID resolution with retry mechanism
     *
     * @param piPath
     */
    private String resolveSingleWebId(String piPath) {
        int maxRetries = 3;
        long baseDelay = 1000; // 1 second base delay

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String encodedPath;
                try {
                    encodedPath = URLEncoder.encode(piPath, StandardCharsets.UTF_8.name());
                } catch (java.io.UnsupportedEncodingException e) {
                    // UTF-8 is always supported, this should never happen
                    throw new RuntimeException("UTF-8 encoding not supported", e);
                }

                // Build correct endpoint URL based on PI Path type
                String endpointUrl = buildEndpointUrl(config.getServerUrl(), piPath);
                String requestUrl = endpointUrl + "?path=" + encodedPath;

                if (log.isDebugEnabled()) {
                    log.debug("Resolving PI Path: {} -> URL: {}", piPath, requestUrl);
                }
                HttpsURLConnection connection = createConnection(requestUrl);
                connection.setRequestMethod("GET");
                connection.setRequestProperty("Accept", "application/json");
                addAuthenticationHeader(connection);

                int responseCode = connection.getResponseCode();
                if (responseCode == 200) {
                    String responseBody = readResponse(connection);
                    JsonNode responseJson = objectMapper.readTree(responseBody);

                    if (attempt > 1) {
                        log.info(
                                "PI Path resolution successful, retry count: {}, Path: {}",
                                attempt - 1,
                                piPath);
                    }
                    return responseJson.path("WebId").asText();
                } else {
                    String errorResponse = readErrorResponse(connection);
                    log.warn(
                            "PI Path resolution failed: {}, HTTP status code: {}, response: {}",
                            piPath,
                            responseCode,
                            errorResponse);
                    return null;
                }
            } catch (javax.net.ssl.SSLHandshakeException e) {
                if (attempt < maxRetries) {
                    long delay = baseDelay * attempt;
                    log.warn(
                            "SSL handshake failed, attempt {}, retrying in {}ms: {}",
                            attempt,
                            delay,
                            e.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry wait interrupted: {}", piPath);
                        return null;
                    }
                } else {
                    log.error(
                            "SSL handshake failed, reached maximum retry count {}: {}",
                            maxRetries,
                            piPath,
                            e);
                    return null;
                }
            } catch (java.io.IOException e) {
                if (attempt < maxRetries && isRetryableIOException(e)) {
                    long delay = baseDelay * attempt;
                    log.warn(
                            "Network IO exception, attempt {}, retrying in {}ms: {}",
                            attempt,
                            delay,
                            e.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry wait interrupted: {}", piPath);
                        return null;
                    }
                } else {
                    log.error("Network IO exception, PI Path: {}", piPath, e);
                    return null;
                }
            } catch (Exception e) {
                log.error("Exception occurred while resolving PI Path: {}", piPath, e);
                return null;
            }
        }

        log.error(
                "PI Path resolution failed, reached maximum retry count {}: {}",
                maxRetries,
                piPath);
        return null;
    }

    /** Determine if it's a retryable IO exception */
    private boolean isRetryableIOException(java.io.IOException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        // Common retryable exceptions
        return message.contains("Connection reset")
                || message.contains("Connection refused")
                || message.contains("Read timed out")
                || message.contains("Connect timed out")
                || message.contains("SSL peer shut down incorrectly")
                || message.contains("Remote host closed connection");
    }

    /** Create HTTPS connection */
    private HttpsURLConnection createConnection(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();

        // Set connection timeout
        connection.setConnectTimeout(config.getConnectionTimeoutMs());
        connection.setReadTimeout(config.getReadTimeoutMs());

        // Set HTTP request properties for better compatibility
        connection.setRequestProperty("User-Agent", "SeaTunnel-PI-Connector/1.0");
        connection.setRequestProperty("Connection", "close"); // Avoid connection reuse issues
        connection.setRequestProperty("Cache-Control", "no-cache");

        // Configure SSL, disable hostname verification and certificate validation (for
        // development/testing environment only)
        if (config.isTrustAllCerts()) {
            try {
                connection.setSSLSocketFactory(createSSLSocketFactory());
                connection.setHostnameVerifier((hostname, session) -> true);
            } catch (Exception e) {
                log.warn(
                        "SSL connection configuration failed, using default settings: {}",
                        e.getMessage());
                throw new IOException("SSL configuration failed: " + e.getMessage(), e);
            }
        }

        return connection;
    }

    /** Create SSL Socket Factory */
    private javax.net.ssl.SSLSocketFactory createSSLSocketFactory() throws Exception {
        // Use generic TLS protocol, let system negotiate the best version
        SSLContext sslContext = SSLContext.getInstance("TLS");

        // Trust all certificates (production environment should use proper certificate validation)
        TrustManager[] trustAllCerts =
                new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

                        public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                    }
                };

        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        // Return system SocketFactory directly, let TLS protocol negotiate automatically
        return sslContext.getSocketFactory();
    }

    /** Add authentication header */
    private void addAuthenticationHeader(HttpsURLConnection connection) {
        AuthType authType = config.getAuthType();

        switch (authType) {
            case BASIC:
                if (config.getUsername() != null && config.getPassword() != null) {
                    String credentials = config.getUsername() + ":" + config.getPassword();
                    String encodedAuth =
                            Base64.getEncoder()
                                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
                }
                break;
            case BEARER:
                if (config.getBearerToken() != null) {
                    connection.setRequestProperty(
                            "Authorization", "Bearer " + config.getBearerToken());
                }
                break;
            case WINDOWS:
                // Windows integrated authentication is usually handled by the system
                break;
            default:
                log.warn("Unsupported authentication type: {}", authType);
        }
    }

    /** Read response content */
    private String readResponse(HttpsURLConnection connection) throws IOException {
        StringBuilder response = new StringBuilder();
        try (java.io.BufferedReader reader =
                new java.io.BufferedReader(
                        new java.io.InputStreamReader(
                                connection.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }
        return response.toString();
    }

    /** Read error response */
    private String readErrorResponse(HttpsURLConnection connection) {
        try {
            StringBuilder response = new StringBuilder();
            try (java.io.BufferedReader reader =
                    new java.io.BufferedReader(
                            new java.io.InputStreamReader(
                                    connection.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }
            return response.toString();
        } catch (Exception e) {
            return "Unable to read error response: " + e.getMessage();
        }
    }

    /**
     * Configure SSL to trust all certificates Note: This is only for development and testing
     * environments, production environment should use proper certificate validation
     */
    private void configureTrustAllSSL() {
        if (!config.isTrustAllCerts()) {
            return; // If trust all certificates is not enabled in config, do not execute
        }

        try {
            TrustManager[] trustAllCerts =
                    new TrustManager[] {
                        new X509TrustManager() {
                            public X509Certificate[] getAcceptedIssuers() {
                                return null;
                            }

                            public void checkClientTrusted(
                                    X509Certificate[] certs, String authType) {}

                            public void checkServerTrusted(
                                    X509Certificate[] certs, String authType) {}
                        }
                    };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

            log.info("Globally configured to trust all SSL certificates and hostnames");
        } catch (Exception e) {
            log.warn("SSL trust configuration failed", e);
        }
    }
}
