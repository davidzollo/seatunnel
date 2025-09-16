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

package org.apache.seatunnel.connectors.seatunnel.pi.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * PI split strategy for creating intelligent splits with support for dynamic split size adjustment
 */
public class PISplitStrategy {

    private static final Logger log = LoggerFactory.getLogger(PISplitStrategy.class);

    private static final int DEFAULT_WEBIDS_PER_SPLIT = 150;
    private static final int MAX_URL_LENGTH = 4000; // Increased limit for better performance
    private static final int MIN_WEBIDS_PER_SPLIT =
            5; // Minimum split size to prevent over-subdivision

    /**
     * Create splits from PI paths (optimized for parallel processing)
     *
     * @param piPaths PI path list
     * @param startTime Start time
     * @param endTime End time
     * @param maxPathsPerSplit Maximum number of paths per split
     * @param maxSplits Maximum number of splits allowed
     * @param autoAdjustSplitSize Whether to automatically adjust split size
     * @return Split list with paths for later WebID resolution
     */
    public List<PISplit> createSplitsFromPaths(
            List<String> piPaths,
            LocalDateTime startTime,
            LocalDateTime endTime,
            int maxPathsPerSplit,
            int maxSplits,
            boolean autoAdjustSplitSize) {

        if (piPaths == null || piPaths.isEmpty()) {
            log.warn("PI path list is empty, cannot create splits");
            return new ArrayList<>();
        }

        if (maxPathsPerSplit <= 0) {
            maxPathsPerSplit = DEFAULT_WEBIDS_PER_SPLIT;
        }

        log.info(
                "Creating splits from paths, total paths: {}, max paths per split: {}",
                piPaths.size(),
                maxPathsPerSplit);

        List<PISplit> splits = new ArrayList<>();

        for (int i = 0; i < piPaths.size(); i += maxPathsPerSplit) {
            int endIndex = Math.min(i + maxPathsPerSplit, piPaths.size());
            List<String> splitPaths = piPaths.subList(i, endIndex);

            String splitId = "split-" + (splits.size());
            PISplit split = new PISplit(splitId, splitPaths);
            splits.add(split);

            log.info("Created split: {}, path count: {}", splitId, splitPaths.size());
        }

        log.info("Created {} splits for {} paths", splits.size(), piPaths.size());
        return splits;
    }

    /**
     * Create split list
     *
     * @param webIds WebID list
     * @param startTime Start time
     * @param endTime End time
     * @param maxWebIdsPerSplit Maximum number of WebIDs per split
     * @param maxSplits Maximum number of splits allowed
     * @param autoAdjustSplitSize Whether to automatically adjust split size
     * @return Split list
     */
    public List<PISplit> createSplits(
            List<String> webIds,
            LocalDateTime startTime,
            LocalDateTime endTime,
            int maxWebIdsPerSplit,
            int maxSplits,
            boolean autoAdjustSplitSize) {

        if (webIds == null || webIds.isEmpty()) {
            log.warn("WebID list is empty, cannot create splits");
            return new ArrayList<>();
        }

        if (maxWebIdsPerSplit <= 0) {
            maxWebIdsPerSplit = DEFAULT_WEBIDS_PER_SPLIT;
        }

        log.info(
                "Starting to create splits, total WebIDs: {}, max WebIDs per split: {}, max splits: {}, auto adjust: {}",
                webIds.size(),
                maxWebIdsPerSplit,
                maxSplits,
                autoAdjustSplitSize);

        List<PISplit> splits = new ArrayList<>();

        // Split WebID list by split size
        /**
         * Method approach: 1. Split webIds into multiple sublists based on maxWebIdsPerSplit. In
         * this case, if the total number of webids is only 100, then it will only be split into one
         * sublist containing all 100 webids. 2. Estimate URL length for each sublist 3. If URL
         * length exceeds limit, halve maxWebIdsPerSplit and recursively call createSplits method 4.
         * If URL length is within limit, create PISplit object and add to splits list
         */
        for (int i = 0; i < webIds.size(); i += maxWebIdsPerSplit) {
            int endIndex = Math.min(i + maxWebIdsPerSplit, webIds.size());
            List<String> splitWebIds = webIds.subList(i, endIndex);

            // Validate URL length
            int estimatedLength = estimateUrlLength(splitWebIds);
            boolean shouldSubdivide =
                    estimatedLength > MAX_URL_LENGTH
                            && maxWebIdsPerSplit > MIN_WEBIDS_PER_SPLIT
                            && autoAdjustSplitSize
                            && splits.size() < maxSplits;

            if (shouldSubdivide) {
                log.warn(
                        "Split {} URL length ({} chars) exceeds limit ({}), performing subdivision from {} to {} WebIDs per split",
                        i / maxWebIdsPerSplit,
                        estimatedLength,
                        MAX_URL_LENGTH,
                        maxWebIdsPerSplit,
                        maxWebIdsPerSplit / 2);
                // Recursive subdivision with all constraints
                splits.addAll(
                        createSplits(
                                splitWebIds,
                                startTime,
                                endTime,
                                maxWebIdsPerSplit / 2,
                                maxSplits,
                                autoAdjustSplitSize));
            } else {
                if (estimatedLength > MAX_URL_LENGTH) {
                    log.warn(
                            "Split {} URL length ({} chars) exceeds limit ({}) but cannot subdivide further (min size: {}), proceeding anyway",
                            i / maxWebIdsPerSplit,
                            estimatedLength,
                            MAX_URL_LENGTH,
                            MIN_WEBIDS_PER_SPLIT);
                }
                String splitId = "split-" + (i / maxWebIdsPerSplit);
                PISplit split = new PISplit(splitId, new ArrayList<>(splitWebIds));
                splits.add(split);

                log.info(
                        "Created split: {}, WebID count: {}, estimated URL length: {} chars",
                        splitId,
                        splitWebIds.size(),
                        estimatedLength);
            }
        }

        log.info("Split creation completed, total splits: {}", splits.size());
        return splits;
    }

    /**
     * Estimate URL length for determining whether split subdivision is needed Improved estimation
     * based on actual PI Web API URL format
     */
    private int estimateUrlLength(List<String> webIds) {
        if (webIds == null || webIds.isEmpty()) {
            return 0;
        }

        // Base URL length estimation (more accurate)
        // Example: https://server:8443/piwebapi/streamsets/recorded?
        int baseUrlLength = 200; // Base URL part with some buffer
        int paramLength = 0;

        for (String webId : webIds) {
            // More accurate parameter estimation: webid=F1DP...&
            // WebIDs are typically 40-50 characters long
            paramLength += 7 + (webId != null ? webId.length() : 45) + 1; // "webid=" + webId + "&"
        }

        // Add time parameter length estimation (more accurate)
        // startTime=2025-08-29T09:00:00Z&endTime=2025-08-29T09:20:00Z&
        int timeParamLength = 80; // startTime and endTime parameters

        // Add other common parameters (maxCount, boundaryType, etc.)
        int otherParamLength = 50;

        int totalLength = baseUrlLength + paramLength + timeParamLength + otherParamLength;

        log.debug(
                "URL length estimation: base={}, params={}, time={}, other={}, total={}",
                baseUrlLength,
                paramLength,
                timeParamLength,
                otherParamLength,
                totalLength);

        return totalLength;
    }
}
