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

import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** PI WebID batch resolver for batch resolving PI paths to WebID */
public class PIWebIdBatchResolver {

    private static final Logger log = LoggerFactory.getLogger(PIWebIdBatchResolver.class);

    /** PI Web API endpoints */
    /** PI Web API endpoints for different resource types */
    private static final String PIWEBAPI_POINTS_ENDPOINT = "/piwebapi/points";

    private static final String PIWEBAPI_POINTS_MULTIPLE_ENDPOINT = "/piwebapi/points/multiple";
    private static final String PIWEBAPI_ATTRIBUTES_ENDPOINT = "/piwebapi/attributes";
    private static final String PIWEBAPI_ATTRIBUTES_MULTIPLE_ENDPOINT =
            "/piwebapi/attributes/multiple";

    /** Default cache size for WebID type information */
    private static final int DEFAULT_WEBID_CACHE_SIZE = 10000;

    private final PIHttpClient httpClient;
    private final PIConfigHelper config;
    private final int batchSize;

    /** WebID type cache manager for optimized API calls */
    private final WebIdTypeCache webIdTypeCache;

    public PIWebIdBatchResolver(PIHttpClient httpClient, PIConfigHelper config) {
        this.httpClient = httpClient;
        this.config = config;
        this.batchSize = config.getWebIdResolveBatchSize();
        this.webIdTypeCache = new WebIdTypeCache(DEFAULT_WEBID_CACHE_SIZE);
    }

    /**
     * Batch resolve PI Paths to WebIDs. Optimization: use batch API to reduce network request count
     */
    public List<String> batchResolveWebIds(List<String> piPaths) {
        List<String> webIds = new ArrayList<>();

        // Process in batches to avoid oversized single requests
        for (int i = 0; i < piPaths.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, piPaths.size());
            List<String> batch = piPaths.subList(i, endIndex);

            try {
                List<String> batchWebIds = resolveBatch(batch);
                webIds.addAll(batchWebIds);

                log.debug(
                        "Batch pi paths to WebID completed, current batch: {}, total batch: {}",
                        endIndex,
                        piPaths.size());

                // Avoid too frequent requests
                Thread.sleep(config.getWebIdResolveDelayMs());

            } catch (Exception e) {
                log.error(
                        "Batch WebID resolution failed, batch: {}-{}, paths: {}",
                        i,
                        endIndex,
                        batch,
                        e);
                throw new RuntimeException(
                        "Batch WebID resolution failed for paths: "
                                + batch
                                + ". Error: "
                                + e.getMessage(),
                        e);
            }
        }

        return webIds;
    }

    /** Batch resolve PI Paths to WebID mapping, including metadata information */
    public Map<String, PIWebIdMetadata> batchResolveWebIdMetadata(List<String> piPaths) {
        Map<String, PIWebIdMetadata> webIdMetadataMap = new HashMap<>();

        log.info(
                "Starting batch WebID metadata resolution, total paths: {}, batch size: {}",
                piPaths.size(),
                batchSize);

        // Process in batches to avoid oversized single requests
        for (int i = 0; i < piPaths.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, piPaths.size());
            List<String> batch = piPaths.subList(i, endIndex);

            try {
                Map<String, PIWebIdMetadata> batchMetadata = resolveBatchMetadata(batch);
                webIdMetadataMap.putAll(batchMetadata);

                // Avoid too frequent requests
                Thread.sleep(config.getWebIdResolveDelayMs());

            } catch (Exception e) {
                log.error(
                        "Batch WebID metadata resolution failed, batch: {}-{}, paths: {}",
                        i,
                        endIndex,
                        batch,
                        e);
                throw new RuntimeException(
                        "Batch WebID metadata resolution failed for paths: "
                                + batch
                                + ". Error: "
                                + e.getMessage(),
                        e);
            }
        }

        log.info("WebID metadata resolution completed, total count: {}", webIdMetadataMap.size());
        return webIdMetadataMap;
    }

    /** Batch resolve a batch of PI paths */
    private List<String> resolveBatch(List<String> piPaths) throws Exception {
        StringBuilder endpointBuilder = new StringBuilder();

        // Select API based on path type
        if (isAttributePath(piPaths.get(0))) {
            endpointBuilder.append(PIWEBAPI_ATTRIBUTES_MULTIPLE_ENDPOINT).append("?");
        } else {
            endpointBuilder.append(PIWEBAPI_POINTS_MULTIPLE_ENDPOINT).append("?");
        }

        for (int i = 0; i < piPaths.size(); i++) {
            if (i > 0) endpointBuilder.append("&");
            endpointBuilder.append("path=").append(URLEncoder.encode(piPaths.get(i), "UTF-8"));
        }

        JsonNode response = httpClient.get(endpointBuilder.toString(), JsonNode.class);
        return extractWebIds(response);
    }

    /** Batch resolve a batch of PI path metadata */
    private Map<String, PIWebIdMetadata> resolveBatchMetadata(List<String> piPaths)
            throws Exception {
        StringBuilder endpointBuilder = new StringBuilder();

        // Select API based on path type
        if (isAttributePath(piPaths.get(0))) {
            endpointBuilder.append(PIWEBAPI_ATTRIBUTES_MULTIPLE_ENDPOINT).append("?");
        } else {
            endpointBuilder.append(PIWEBAPI_POINTS_MULTIPLE_ENDPOINT).append("?");
        }

        for (int i = 0; i < piPaths.size(); i++) {
            if (i > 0) endpointBuilder.append("&");
            endpointBuilder.append("path=").append(URLEncoder.encode(piPaths.get(i), "UTF-8"));
        }

        JsonNode response = httpClient.get(endpointBuilder.toString(), JsonNode.class);
        return extractWebIdMetadata(response, piPaths);
    }

    /** Extract WebID list from response */
    private List<String> extractWebIds(JsonNode response) {
        List<String> webIds = new ArrayList<>();

        if (response == null) {
            return webIds;
        }

        // Handle single object response
        if (response.has("WebId")) {
            webIds.add(response.get("WebId").asText());
        }

        // Handle array response
        if (response.has("Items") && response.get("Items").isArray()) {
            for (JsonNode item : response.get("Items")) {
                if (item.has("WebId")) {
                    webIds.add(item.get("WebId").asText());
                } else if (item.has("Object") && item.get("Object").has("WebId")) {
                    webIds.add(item.get("Object").get("WebId").asText());
                }
            }
        }

        return webIds;
    }

    /** Extract WebID metadata mapping from response */
    private Map<String, PIWebIdMetadata> extractWebIdMetadata(
            JsonNode response, List<String> originalPaths) {
        Map<String, PIWebIdMetadata> metadataMap = new HashMap<>();

        if (response == null) {
            log.warn("Response is null, returning empty metadata map");
            return metadataMap;
        }

        log.debug("Extracting metadata from response: {}", response.toString());

        // Handle array response
        if (response.has("Items") && response.get("Items").isArray()) {
            JsonNode items = response.get("Items");
            for (int i = 0; i < items.size() && i < originalPaths.size(); i++) {
                JsonNode item = items.get(i);
                String webId = null;
                String name = null;

                if (item.has("WebId")) {
                    webId = item.get("WebId").asText();
                    name =
                            item.has("Name")
                                    ? item.get("Name").asText()
                                    : extractNameFromPath(originalPaths.get(i));
                    String pathVal =
                            item.has("Path") ? item.get("Path").asText() : originalPaths.get(i);
                    if (webId != null) {
                        metadataMap.put(webId, new PIWebIdMetadata(webId, name, pathVal));
                        // Cache the type information for this WebID
                        webIdTypeCache.putType(webId, isAttributePath(originalPaths.get(i)));
                    }
                } else if (item.has("Object") && item.get("Object").has("WebId")) {
                    JsonNode objectNode = item.get("Object");
                    webId = objectNode.get("WebId").asText();
                    name =
                            objectNode.has("Name")
                                    ? objectNode.get("Name").asText()
                                    : extractNameFromPath(originalPaths.get(i));
                    String pathVal =
                            objectNode.has("Path")
                                    ? objectNode.get("Path").asText()
                                    : originalPaths.get(i);
                    if (webId != null) {
                        metadataMap.put(webId, new PIWebIdMetadata(webId, name, pathVal));
                        // Cache the type information for this WebID
                        webIdTypeCache.putType(webId, isAttributePath(originalPaths.get(i)));
                    }
                }
            }
        }

        // Log cache statistics for monitoring
        if (log.isDebugEnabled()) {
            log.debug("WebID type cache statistics: {}", webIdTypeCache.getStatistics());
        }

        return metadataMap;
    }

    /** Extract name from PI path */
    private String extractNameFromPath(String piPath) {
        if (piPath == null || piPath.isEmpty()) {
            log.warn("PI path is null or empty, returning Unknown");
            return "Unknown";
        }

        // For AF Attribute path: \\PI-AFServer01&02\WebAPI\Test|Attribute1
        if (piPath.contains("|")) {
            String[] parts = piPath.split("\\|");
            String name = parts.length > 1 ? parts[parts.length - 1] : "Unknown";
            log.info("AF Attribute path detected, extracted name: {}", name);
            return name;
        }

        // For PI Point path: \\pims.huafeng.com\HF.AA.NAB:LIA-26101.PV
        if (piPath.contains("\\")) {
            String[] parts = piPath.split("\\\\");
            String name = parts.length > 0 ? parts[parts.length - 1] : "Unknown";
            log.info("PI Point path detected, extracted name: {}", name);
            return name;
        }

        log.info("No special path pattern detected, using full path as name: {}", piPath);
        return piPath;
    }

    /** Determine if it's an AF Attribute path */
    private boolean isAttributePath(String path) {
        return path.contains("|");
    }

    /** Batch resolve WebID metadata by WebIDs (for splits containing WebIDs) */
    public Map<String, PIWebIdMetadata> batchResolveWebIdMetadataByWebIds(List<String> webIds)
            throws Exception {
        Map<String, PIWebIdMetadata> metadataMap = new HashMap<>();

        for (String webId : webIds) {
            try {
                // Resolve WebID metadata with intelligent type detection
                PIWebIdMetadata metadata = resolveWebIdMetadata(webId);
                if (metadata != null) {
                    metadataMap.put(webId, metadata);
                }
            } catch (Exception e) {
                log.warn("Failed to resolve metadata for WebID {}: {}", webId, e.getMessage());
            }
        }

        return metadataMap;
    }

    /**
     * Resolve single WebID metadata with intelligent type detection.
     *
     * <p>This method uses cached type information when available to make precise API calls, falling
     * back to automatic detection when cache miss occurs.
     *
     * @param webId the WebID to resolve
     * @return PIWebIdMetadata containing WebID, name and path information
     * @throws Exception if API call fails or WebID is invalid
     */
    public PIWebIdMetadata resolveWebIdMetadata(String webId) throws Exception {
        // Check if we have cached type information for this WebID
        Boolean isAttribute = webIdTypeCache.getType(webId);

        if (isAttribute != null) {
            // We have cached type info, use it to make precise API call
            String endpoint =
                    isAttribute
                            ? PIWEBAPI_ATTRIBUTES_ENDPOINT + "/" + webId
                            : PIWEBAPI_POINTS_ENDPOINT + "/" + webId;

            JsonNode response = httpClient.get(endpoint, JsonNode.class);

            if (response != null) {
                String name = response.has("Name") ? response.get("Name").asText() : webId;
                String path = response.has("Path") ? response.get("Path").asText() : webId;
                return new PIWebIdMetadata(webId, name, path);
            }
        }

        // Fallback: no cached type info, try Points first then Attributes
        String endpoint = PIWEBAPI_POINTS_ENDPOINT + "/" + webId;
        JsonNode response = httpClient.get(endpoint, JsonNode.class);

        if (response != null) {
            String name = response.has("Name") ? response.get("Name").asText() : webId;
            String path = response.has("Path") ? response.get("Path").asText() : webId;
            return new PIWebIdMetadata(webId, name, path);
        }

        // If Points failed, try Attributes API
        endpoint = PIWEBAPI_ATTRIBUTES_ENDPOINT + "/" + webId;
        response = httpClient.get(endpoint, JsonNode.class);

        if (response == null) {
            return null;
        }

        String name = response.has("Name") ? response.get("Name").asText() : webId;
        String path = response.has("Path") ? response.get("Path").asText() : webId;

        return new PIWebIdMetadata(webId, name, path);
    }
}
