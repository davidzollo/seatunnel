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

package org.apache.seatunnel.connectors.seatunnel.pimetadata.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIHttpClient;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;
import org.apache.seatunnel.connectors.seatunnel.pi.utils.PIDataTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.pimetadata.split.PIMetadataSplit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class PIMetadataSourceReader implements SourceReader<SeaTunnelRow, PIMetadataSplit> {

    private final Context context;
    private final PIConfigHelper configHelper;
    private final SeaTunnelRowType rowType;
    private final Deque<PIMetadataSplit> pendingSplits;
    private PIHttpClient httpClient;
    private volatile boolean noMoreSplits = false;

    public PIMetadataSourceReader(
            Context context, PIConfigHelper configHelper, SeaTunnelRowType rowType) {
        this.context = context;
        this.configHelper = configHelper;
        this.rowType = rowType;
        this.pendingSplits = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void open() throws Exception {

        // Initialize connector-pi HTTP client
        this.httpClient = new PIHttpClient(configHelper);
    }

    @Override
    public void close() throws IOException {

        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            PIMetadataSplit split = pendingSplits.poll();
            if (split != null) {
                processSplit(split, output);
            }
        }

        // Check if all splits processing is complete, process mode notification framework complete
        if (noMoreSplits
                && pendingSplits.isEmpty()
                && context.getBoundedness()
                        == org.apache.seatunnel.api.source.Boundedness.BOUNDED) {
            log.info("All splits processed, signaling completion for bounded PI metadata source");

            context.signalNoMoreElement();
            return;
        } else {
            log.error(
                    "noMoreSplits:{},pendingSplits.isEmpty:{},context.getBoundedness:{}",
                    noMoreSplits,
                    pendingSplits.isEmpty(),
                    context.getBoundedness());
        }

        Thread.sleep(100);
    }

    @Override
    public List<PIMetadataSplit> snapshotState(long checkpointId) {

        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<PIMetadataSplit> splits) {

        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("No more splits will be assigned to this reader");
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Checkpoint {} completed", checkpointId);
    }

    private void processSplit(PIMetadataSplit split, Collector<SeaTunnelRow> output)
            throws Exception {

        try {
            // Batch get meta data
            JsonNode response = getMetadataForPaths(split.getPiPaths());

            // Convert connector-pi data
            List<SeaTunnelRow> rows =
                    PIDataTypeConverter.parseBatchResponse(
                            response.toString(), rowType, configHelper.getJsonField());

            for (SeaTunnelRow row : rows) {
                output.collect(row);
            }

            log.info(
                    "Successfully processed split {} and collected {} rows",
                    split.splitId(),
                    rows.size());
            split.markCompleted();

        } catch (Exception e) {
            log.error("Failed to process split {}: {}", split.splitId(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Get metadata based on configured metadata type Uses the metadata_type configuration parameter
     * to determine whether to fetch PI Points or AF Attributes
     */
    private JsonNode getMetadataForPaths(List<String> piPaths) throws Exception {
        if (piPaths.isEmpty()) {
            throw new IllegalArgumentException("PI paths cannot be empty");
        }

        // Use configured metadata type (required configuration)
        switch (configHelper.getMetadataType()) {
            case POINTS:
                return getPointsMetadata(piPaths);
            case ATTRIBUTES:
                return getAttributesMetadata(piPaths);
            default:
                throw new IllegalArgumentException(
                        "Unsupported metadata type: " + configHelper.getMetadataType());
        }
    }

    /** Batch get PI Points data * */
    private JsonNode getPointsMetadata(List<String> piPaths) throws Exception {
        // Check and process (URL) to avoid URL length limit
        List<List<String>> batches = splitPathsIfNeeded(piPaths);

        if (batches.size() == 1) {
            // Process single batch
            return getPointsMetadataBatch(batches.get(0));
        } else {
            // Process and merge batch results to avoid URL length limit
            log.info(
                    "Split {} paths into {} batches to avoid URL length limit",
                    piPaths.size(),
                    batches.size());
            return mergeMetadataResults(batches, this::getPointsMetadataBatch);
        }
    }

    /** Batch get PI Points data * */
    private JsonNode getPointsMetadataBatch(List<String> piPaths) throws Exception {
        // Batch get PI Points data
        StringBuilder pathsParam = new StringBuilder();
        for (int i = 0; i < piPaths.size(); i++) {
            if (i > 0) pathsParam.append("&");
            pathsParam.append("path=").append(java.net.URLEncoder.encode(piPaths.get(i), "UTF-8"));
        }

        String endpoint = "/piwebapi/points/multiple?" + pathsParam.toString();

        // Check GET request URL length
        if (endpoint.length() > 4000) {
            log.warn(
                    "GET request URL length {} exceeds safety limit, suggestion to reduce batch size",
                    endpoint.length());
        }

        return httpClient.getJson(endpoint);
    }

    /** Batch get AF Attributes data * */
    private JsonNode getAttributesMetadata(List<String> piPaths) throws Exception {
        // Check and process (URL) to avoid URL length limit
        List<List<String>> batches = splitPathsIfNeeded(piPaths);

        if (batches.size() == 1) {
            // Process single batch
            return getAttributesMetadataBatch(batches.get(0));
        } else {
            // Process and merge batch results to avoid URL length limit
            log.info(
                    "Split {} paths into {} batches to avoid URL length limit",
                    piPaths.size(),
                    batches.size());
            return mergeMetadataResults(batches, this::getAttributesMetadataBatch);
        }
    }

    /** Batch get AF Attributes data * */
    private JsonNode getAttributesMetadataBatch(List<String> piPaths) throws Exception {
        // Batch get AF Attributes data
        StringBuilder pathsParam = new StringBuilder();
        for (int i = 0; i < piPaths.size(); i++) {
            if (i > 0) pathsParam.append("&");
            pathsParam.append("path=").append(java.net.URLEncoder.encode(piPaths.get(i), "UTF-8"));
        }

        String endpoint = "/piwebapi/attributes/multiple?" + pathsParam.toString();

        // Check GET request URL length
        if (endpoint.length() > 4000) {
            log.warn(
                    "GET request URL length {} exceeds safety limit, suggestion to reduce batch size",
                    endpoint.length());
        }

        return httpClient.getJson(endpoint);
    }

    /**
     * Process paths with GET request URL length restriction optimization
     *
     * <p>PI Web API metadata supports GET request, control URL length
     */
    private List<List<String>> splitPathsIfNeeded(List<String> piPaths) {
        List<List<String>> batches = new ArrayList<>();
        List<String> currentBatch = new ArrayList<>();
        int currentUrlLength = 100; // Base URL length

        for (String path : piPaths) {
            // Estimate encoding length (based on UTF-8 encoding)
            int encodedLength = path.length() * 3; // URL encoding
            int paramLength = encodedLength + 15; // "path=" + "&" + Safety margin

            // Check if adding path exceeds GET request URL length restriction
            if (currentUrlLength + paramLength > 4000 && !currentBatch.isEmpty()) {
                // Current batch full, start new batch
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
                currentUrlLength = 100;
                log.info("URL length limit batching: Current batch full, starting new batch");
            }

            currentBatch.add(path);
            currentUrlLength += paramLength;
        }

        // Add last batch
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        return batches;
    }

    /** Merge data results */
    private JsonNode mergeMetadataResults(List<List<String>> batches, MetadataFetcher fetcher)
            throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode mergedItems = mapper.createArrayNode();

        for (List<String> batch : batches) {
            JsonNode batchResult = fetcher.fetch(batch);
            if (batchResult.has("Items") && batchResult.get("Items").isArray()) {
                ArrayNode items = (ArrayNode) batchResult.get("Items");
                for (JsonNode item : items) {
                    mergedItems.add(item);
                }
            }
        }

        // Create merge result
        ObjectNode result = mapper.createObjectNode();
        result.set("Items", mergedItems);
        result.put("Links", "{}");

        return result;
    }

    @FunctionalInterface
    private interface MetadataFetcher {
        JsonNode fetch(List<String> paths) throws Exception;
    }
}
