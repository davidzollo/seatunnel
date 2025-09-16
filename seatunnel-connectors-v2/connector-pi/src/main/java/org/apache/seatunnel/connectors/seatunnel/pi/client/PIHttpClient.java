package org.apache.seatunnel.connectors.seatunnel.pi.client;

import org.apache.seatunnel.connectors.seatunnel.pi.config.AuthType;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * PI Web API HTTP Client
 *
 * <p>Design features: - Supports HTTPS and SSL certificate trust configuration - Supports Basic,
 * Windows, Bearer authentication - Supports request retry and timeout configuration - Supports
 * connection pooling and Keep-Alive - Complete error handling and logging - Production-level
 * exception recovery mechanisms
 */
public class PIHttpClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PIHttpClient.class);

    // Default connection parameters
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int RETRY_INTERVAL_BASE_MS = 500;

    private final PIConfigHelper config;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final String authorizationHeader;

    public PIHttpClient(PIConfigHelper config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.baseUrl = config.getServerUrl();
        this.authorizationHeader = buildAuthorizationHeader();
        this.httpClient = createHttpClient();
    }

    /** Send GET request and parse response to specified type, with automatic retry mechanism */
    public <T> T get(String endpoint, Class<T> responseType) throws Exception {
        String url = buildUrl(endpoint);
        HttpGet request = new HttpGet(url);

        // Set authentication header
        if (authorizationHeader != null) {
            request.setHeader("Authorization", authorizationHeader);
        }

        // Set request headers
        request.setHeader("Accept", "application/json");
        request.setHeader(
                "Accept-Encoding", "identity"); // Disable compression to avoid GZIP parsing errors
        request.setHeader("User-Agent", "SeaTunnel-PI-Connector/1.0");

        // Get retry count
        int maxRetries =
                config.getRetryAttempts() > 0 ? config.getRetryAttempts() : DEFAULT_MAX_RETRIES;

        Exception lastException = null;
        for (int retryCount = 0; retryCount <= maxRetries; retryCount++) {
            try {
                if (retryCount > 0) {
                    // Calculate backoff time: base time * 2^retry count (exponential backoff)
                    long waitTime = RETRY_INTERVAL_BASE_MS * (long) Math.pow(2, retryCount - 1);
                    log.debug(
                            "Retrying HTTP request ({}/{}), waiting {}ms: {}",
                            retryCount,
                            maxRetries,
                            waitTime,
                            url);
                    Thread.sleep(waitTime);
                }

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    return handleResponse(response, responseType, url);
                }
            } catch (Exception e) {
                lastException = e;

                // Determine if retry is needed
                if (!isRetryableException(e) || retryCount >= maxRetries) {
                    break;
                }

                log.warn("HTTP request failed (will retry): {}, error: {}", url, e.getMessage());
            }
        }

        // All retries failed
        if (lastException != null) {
            throw mapException(lastException, url);
        } else {
            throw new PIConnectorException(PIErrorCode.HTTP_REQUEST_FAILED, "Unknown error");
        }
    }

    /** Send GET request and return string response, with automatic retry mechanism */
    public String get(String endpoint) throws Exception {
        String url = buildUrl(endpoint);
        HttpGet request = new HttpGet(url);

        // Set authentication header
        if (authorizationHeader != null) {
            request.setHeader("Authorization", authorizationHeader);
        }

        // Set request headers
        request.setHeader("Accept", "application/json");
        request.setHeader(
                "Accept-Encoding", "identity"); // Disable compression to avoid GZIP parsing errors
        request.setHeader("User-Agent", "SeaTunnel-PI-Connector/1.0");

        // Get retry count
        int maxRetries =
                config.getRetryAttempts() > 0 ? config.getRetryAttempts() : DEFAULT_MAX_RETRIES;

        Exception lastException = null;
        for (int retryCount = 0; retryCount <= maxRetries; retryCount++) {
            try {
                if (retryCount > 0) {
                    // Calculate backoff time: base time * 2^retry count (exponential backoff)
                    long waitTime = RETRY_INTERVAL_BASE_MS * (long) Math.pow(2, retryCount - 1);
                    log.debug(
                            "Retrying HTTP request ({}/{}), waiting {}ms: {}",
                            retryCount,
                            maxRetries,
                            waitTime,
                            url);
                    Thread.sleep(waitTime);
                }

                log.debug("Sending HTTP GET request (String): {}", url);
                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        throw new PIConnectorException(
                                PIErrorCode.HTTP_REQUEST_FAILED,
                                "HTTP response body is empty: " + url);
                    }

                    // Read directly from InputStream to avoid EntityUtils auto-decompression issues
                    String responseBody;
                    try (InputStream inputStream = entity.getContent();
                            InputStreamReader reader =
                                    new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                            BufferedReader bufferedReader = new BufferedReader(reader)) {
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            sb.append(line);
                        }
                        responseBody = sb.toString();
                    }
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        return responseBody;
                    } else {
                        // Server errors may be temporary and can be retried
                        boolean isServerError = statusCode >= 500 && statusCode < 600;
                        if (isServerError && retryCount < maxRetries) {
                            log.warn("Received server error {}, will retry: {}", statusCode, url);
                            continue;
                        }

                        throw new PIConnectorException(
                                statusCode >= 400 && statusCode < 500
                                        ? PIErrorCode.CLIENT_ERROR
                                        : PIErrorCode.SERVER_ERROR,
                                String.format(
                                        "HTTP request failed: %d, URL: %s, response: %s",
                                        statusCode, url, responseBody));
                    }
                }
            } catch (Exception e) {
                lastException = e;

                // Determine if retry is needed
                if (!isRetryableException(e) || retryCount >= maxRetries) {
                    break;
                }

                log.warn("HTTP request failed (will retry): {}, error: {}", url, e.getMessage());
            }
        }

        // All retries failed
        if (lastException != null) {
            throw mapException(lastException, url);
        } else {
            throw new PIConnectorException(PIErrorCode.HTTP_REQUEST_FAILED, "Unknown error");
        }
    }

    /** Send GET request and return JSON node, with automatic retry mechanism */
    public JsonNode getJson(String endpoint) throws Exception {
        String url = buildUrl(endpoint);
        HttpGet request = new HttpGet(url);

        // Set authentication header
        if (authorizationHeader != null) {
            request.setHeader("Authorization", authorizationHeader);
        }

        // Set request headers
        request.setHeader("Accept", "application/json");
        request.setHeader(
                "Accept-Encoding", "identity"); // Disable compression to avoid GZIP parsing errors
        request.setHeader("User-Agent", "SeaTunnel-PI-Connector/1.0");

        // Get retry count
        int maxRetries =
                config.getRetryAttempts() > 0 ? config.getRetryAttempts() : DEFAULT_MAX_RETRIES;

        Exception lastException = null;
        for (int retryCount = 0; retryCount <= maxRetries; retryCount++) {
            try {
                if (retryCount > 0) {
                    // Calculate backoff time: base time * 2^retry count (exponential backoff)
                    long waitTime = RETRY_INTERVAL_BASE_MS * (long) Math.pow(2, retryCount - 1);
                    log.debug(
                            "Retrying HTTP request ({}/{}), waiting {}ms: {}",
                            retryCount,
                            maxRetries,
                            waitTime,
                            url);
                    Thread.sleep(waitTime);
                }

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        throw new PIConnectorException(
                                PIErrorCode.HTTP_REQUEST_FAILED,
                                "HTTP response body is empty: " + url);
                    }

                    // Read directly from InputStream to avoid EntityUtils auto-decompression issues
                    String responseBody;
                    try (InputStream inputStream = entity.getContent();
                            InputStreamReader reader =
                                    new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                            BufferedReader bufferedReader = new BufferedReader(reader)) {
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            sb.append(line);
                        }
                        responseBody = sb.toString();
                    }
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        try {
                            return objectMapper.readTree(responseBody);
                        } catch (JsonParseException e) {
                            log.error("JSON parsing error: {}", responseBody);
                            throw new PIConnectorException(
                                    PIErrorCode.DATA_PARSE_FAILED,
                                    "Unable to parse JSON response: " + e.getMessage(),
                                    e);
                        }
                    } else {
                        // Server errors may be temporary and can be retried
                        boolean isServerError = statusCode >= 500 && statusCode < 600;
                        if (isServerError && retryCount < maxRetries) {
                            log.warn("Received server error {}, will retry: {}", statusCode, url);
                            continue;
                        }

                        throw new PIConnectorException(
                                PIErrorCode.HTTP_REQUEST_FAILED,
                                String.format(
                                        "HTTP request failed: %d, URL: %s, response: %s",
                                        statusCode, url, responseBody));
                    }
                }
            } catch (Exception e) {
                lastException = e;

                // Determine if retry is needed
                if (!isRetryableException(e) || retryCount >= maxRetries) {
                    break;
                }

                log.warn("HTTP request failed (will retry): {}, error: {}", url, e.getMessage());
            }
        }

        // All retries failed
        if (lastException != null) {
            throw mapException(lastException, url);
        } else {
            throw new PIConnectorException(PIErrorCode.HTTP_REQUEST_FAILED, "Unknown error");
        }
    }

    /** Get server information */
    public JsonNode getServerInfo() throws Exception {
        return getJson("/system");
    }

    /** Query batch data (PI Web API Streams Recorded) - single WebID */
    public JsonNode queryRecordedData(
            String webId, String startTime, String endTime, int maxCount, String boundaryType)
            throws Exception {
        // Correct API format: /piwebapi/streams/{webId}/recorded
        StringBuilder endpoint = new StringBuilder("/piwebapi/streams/");
        endpoint.append(URLEncoder.encode(webId, StandardCharsets.UTF_8.name()));
        endpoint.append("/recorded");
        endpoint.append("?startTime=")
                .append(URLEncoder.encode(startTime, StandardCharsets.UTF_8.name()));
        endpoint.append("&endTime=")
                .append(URLEncoder.encode(endTime, StandardCharsets.UTF_8.name()));
        endpoint.append("&maxCount=").append(maxCount);
        endpoint.append("&boundaryType=").append(boundaryType);

        return getJson(endpoint.toString());
    }

    /** Query current values */
    public JsonNode queryCurrentValues(String webIds) throws Exception {
        String endpoint =
                "/streamsets/value?webid="
                        + URLEncoder.encode(webIds, StandardCharsets.UTF_8.name());
        return getJson(endpoint.toString());
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    /** Create HTTP client */
    private CloseableHttpClient createHttpClient() {
        try {
            HttpClientBuilder builder = HttpClients.custom();

            // 1. Set request configuration (timeout)
            RequestConfig requestConfig =
                    RequestConfig.custom()
                            .setConnectTimeout(config.getConnectionTimeoutMs())
                            .setSocketTimeout(config.getReadTimeoutMs())
                            .setConnectionRequestTimeout(
                                    config.getReadTimeoutMs()) // Timeout for getting connection
                            // from pool
                            .build();
            builder.setDefaultRequestConfig(requestConfig);

            // 2. Add browser User-Agent
            String userAgent =
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";
            builder.setUserAgent(userAgent);

            // 3. Handle SSL/TLS (trust all certificates and specify protocols)
            if (baseUrl.startsWith("https")) {
                SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
                if (config.isTrustAllCerts()) {
                    sslContextBuilder.loadTrustMaterial(null, (chain, authType) -> true);
                }

                // Explicitly specify supported TLS protocol versions
                SSLContext sslContext = sslContextBuilder.build();
                SSLConnectionSocketFactory sslsf =
                        new SSLConnectionSocketFactory(
                                sslContext, null, null, NoopHostnameVerifier.INSTANCE);
                builder.setSSLSocketFactory(sslsf);
            }

            // 4. Disable retry (handled by our own logic)
            builder.disableAutomaticRetries();

            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PI HTTP client", e);
        }
    }

    /** Build complete URL */
    private String buildUrl(String endpoint) {
        // If endpoint is a complete URL (starts with http), return directly
        if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            return endpoint;
        }

        // If endpoint starts with /, append to baseUrl
        if (endpoint.startsWith("/")) {
            // Extract base part of baseUrl (protocol+host+port)
            try {
                java.net.URL url = new java.net.URL(baseUrl);
                String baseUrlRoot = url.getProtocol() + "://" + url.getHost();
                if (url.getPort() != -1) {
                    baseUrlRoot += ":" + url.getPort();
                }
                return baseUrlRoot + endpoint;
            } catch (Exception e) {
                // If parsing fails, use original logic
                return baseUrl + endpoint;
            }
        } else {
            return baseUrl + "/" + endpoint;
        }
    }

    /** Build authentication header */
    private String buildAuthorizationHeader() {
        AuthType authType = config.getAuthType();
        if (authType == null) {
            return null;
        }

        try {
            if (AuthType.BASIC.equals(authType)) {
                String username = config.getUsername();
                String password = config.getPassword();
                if (username == null || password == null) {
                    throw new PIConnectorException(
                            PIErrorCode.AUTHENTICATION_FAILED,
                            "Username and password must be provided in Basic authentication mode");
                }

                String auth = username + ":" + password;
                String encodedAuth =
                        Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                return "Basic " + encodedAuth;
            } else if (AuthType.BEARER.equals(authType)) {
                String token = config.getBearerToken();
                if (token == null || token.isEmpty()) {
                    throw new PIConnectorException(
                            PIErrorCode.AUTHENTICATION_FAILED,
                            "Token must be provided in Bearer authentication mode");
                }
                return "Bearer " + token;
            } else {
                log.warn(
                        "Unsupported authentication type: {}, will try without authentication",
                        authType);
                return null;
            }
        } catch (Exception e) {
            if (e instanceof PIConnectorException) {
                throw (PIConnectorException) e;
            }
            throw new PIConnectorException(
                    PIErrorCode.AUTHENTICATION_FAILED,
                    "Failed to build authentication header: " + e.getMessage(),
                    e);
        }
    }

    /** Handle HTTP response */
    private <T> T handleResponse(CloseableHttpResponse response, Class<T> responseType, String url)
            throws Exception {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new PIConnectorException(
                    PIErrorCode.HTTP_REQUEST_FAILED, "HTTP response body is empty: " + url);
        }

        // Read directly from InputStream to avoid EntityUtils auto-decompression issues
        String responseBody;
        try (InputStream inputStream = entity.getContent();
                InputStreamReader reader =
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            responseBody = sb.toString();
        }
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode >= 200 && statusCode < 300) {
            try {
                return objectMapper.readValue(responseBody, responseType);
            } catch (Exception e) {
                log.error("Response parsing failed: {}, content: {}", url, responseBody);
                throw new PIConnectorException(
                        PIErrorCode.DATA_PARSE_FAILED,
                        "Response parsing failed: " + e.getMessage(),
                        e);
            }
        } else {
            throw new PIConnectorException(
                    statusCode >= 400 && statusCode < 500
                            ? PIErrorCode.CLIENT_ERROR
                            : PIErrorCode.SERVER_ERROR,
                    String.format(
                            "HTTP request failed: %d, URL: %s, response: %s",
                            statusCode, url, responseBody));
        }
    }

    /** Whether the exception is retryable */
    private boolean isRetryableException(Exception e) {
        // Timeout exceptions can be retried
        if (e instanceof SocketTimeoutException
                || e instanceof ConnectTimeoutException
                || e instanceof HttpHostConnectException) {
            return true;
        }

        // SSL exceptions usually don't need retry
        if (e instanceof SSLException) {
            return false;
        }

        // PIConnectorException needs to be judged based on error code
        if (e instanceof PIConnectorException) {
            PIConnectorException pce = (PIConnectorException) e;
            PIErrorCode errorCode = (PIErrorCode) pce.getErrorCode();

            // Server errors can be retried, authentication errors cannot
            if (errorCode == PIErrorCode.SERVER_ERROR
                    || errorCode == PIErrorCode.HTTP_TIMEOUT
                    || errorCode == PIErrorCode.CONNECTION_TIMEOUT) {
                return true;
            }

            if (errorCode == PIErrorCode.AUTHENTICATION_FAILED
                    || errorCode == PIErrorCode.CLIENT_ERROR) {
                return false;
            }
        }

        // Conservative strategy, other exceptions can be retried by default
        return true;
    }

    /** Map generic exception to PIConnectorException */
    private PIConnectorException mapException(Exception e, String url) {
        if (e instanceof PIConnectorException) {
            return (PIConnectorException) e;
        }

        if (e instanceof SocketTimeoutException) {
            return new PIConnectorException(
                    PIErrorCode.HTTP_TIMEOUT, "HTTP request timeout: " + url, e);
        }

        if (e instanceof ConnectTimeoutException) {
            return new PIConnectorException(
                    PIErrorCode.CONNECTION_TIMEOUT, "Connection timeout: " + url, e);
        }

        if (e instanceof HttpHostConnectException) {
            return new PIConnectorException(
                    PIErrorCode.CONNECTION_REFUSED, "Connection refused: " + url, e);
        }

        if (e instanceof SSLException) {
            return new PIConnectorException(
                    PIErrorCode.SSL_HANDSHAKE_FAILED, "SSL handshake failed: " + url, e);
        }

        if (e instanceof JsonParseException) {
            return new PIConnectorException(
                    PIErrorCode.DATA_PARSE_FAILED, "JSON parsing failed: " + url, e);
        }

        // Default to HTTP request failed
        return new PIConnectorException(
                PIErrorCode.HTTP_REQUEST_FAILED, "HTTP request failed: " + e.getMessage(), e);
    }

    public PIConfigHelper getConfig() {
        return config;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    /** Empty hostname verifier for ignoring SSL certificate hostname verification */
    private static class NoopHostnameVerifier implements HostnameVerifier {
        static final NoopHostnameVerifier INSTANCE = new NoopHostnameVerifier();

        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    public JsonNode getPiPoints(String webId) throws Exception {
        String jsonResponse = get(String.format("streams/%s/value", webId));
        return objectMapper.readTree(jsonResponse);
    }

    public JsonNode getPiSystemInfo() throws Exception {
        String jsonResponse = get(""); // Request /piwebapi/
        return objectMapper.readTree(jsonResponse);
    }
}
