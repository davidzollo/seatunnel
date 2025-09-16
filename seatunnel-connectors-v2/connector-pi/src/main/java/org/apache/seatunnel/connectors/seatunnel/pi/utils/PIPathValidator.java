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

package org.apache.seatunnel.connectors.seatunnel.pi.utils;

import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pi.exception.PIErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** PI path validator for validating PI path list format, duplicates, etc. */
public class PIPathValidator {

    private static final Logger log = LoggerFactory.getLogger(PIPathValidator.class);

    /**
     * Validate PI path list
     *
     * @param piPaths PI path list
     * @throws PIConnectorException throws exception when validation fails
     */
    public static void validatePiPaths(List<String> piPaths) {
        if (piPaths == null || piPaths.isEmpty()) {
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_VALIDATION_FAILED, "pi_paths configuration cannot be empty");
        }

        log.info("Starting PI path configuration validation, total count: {}", piPaths.size());

        // 1. Check for duplicate paths
        Set<String> uniquePaths = new HashSet<>();
        List<String> duplicates = new ArrayList<>();

        for (String path : piPaths) {
            if (!uniquePaths.add(path)) {
                duplicates.add(path);
            }
        }

        if (!duplicates.isEmpty()) {
            log.error("Found duplicate PI paths, count: {}", duplicates.size());
            for (String duplicate : duplicates) {
                log.error("Duplicate path: {}", duplicate);
            }
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("Found %d duplicate PI paths", duplicates.size()));
        }

        // 2. Check path format
        List<String> invalidPaths = new ArrayList<>();
        int pointCount = 0;
        int attributeCount = 0;

        for (String path : piPaths) {
            if (path == null || path.trim().isEmpty()) {
                invalidPaths.add("Empty path");
                continue;
            }

            String trimmedPath = path.trim();

            // Validate path format
            if (!isValidPiPath(trimmedPath)) {
                invalidPaths.add(trimmedPath);
                continue;
            }

            // Count path types
            if (trimmedPath.contains("|")) {
                attributeCount++;
            } else {
                pointCount++;
            }
        }

        if (!invalidPaths.isEmpty()) {
            log.error("Found invalid PI paths, count: {}", invalidPaths.size());
            for (String invalidPath : invalidPaths) {
                log.error("Invalid path: {}", invalidPath);
            }
            throw new PIConnectorException(
                    PIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("Found %d invalid PI paths", invalidPaths.size()));
        }

        // 3. Output validation results
        StringBuilder validationResult = new StringBuilder();
        validationResult.append("PI path validation passed:\n");
        validationResult.append("  - Total path count: ").append(piPaths.size()).append("\n");
        validationResult.append("  - Unique path count: ").append(uniquePaths.size()).append("\n");
        validationResult.append("  - PI Point paths: ").append(pointCount).append("\n");
        validationResult.append("  - AF Attribute paths: ").append(attributeCount).append("\n");

        // Check for duplicate paths
        boolean hasDuplicates = piPaths.size() != uniquePaths.size();
        validationResult
                .append("  - No duplicate paths: ")
                .append(hasDuplicates ? "FAILED" : "PASSED")
                .append("\n");

        // Format validation already passed (if we reach here, no invalid paths found)
        validationResult.append("  - Format validation passed: PASSED");
        log.info(validationResult.toString());
    }

    /** Validate if PI path format is correct */
    private static boolean isValidPiPath(String path) {
        // PI Point format: \\\\ServerName\\TagName
        // AF Attribute format: \\\\ServerName\\Database\\Element|AttributeName

        if (!path.startsWith("\\")) {
            return false;
        }

        // Check if necessary separators are present
        String[] parts = path.split("\\\\");
        if (parts.length < 3) { // At least need "", "", "ServerName", "..."
            return false;
        }

        // Check that server name is not empty
        if (parts.length >= 3 && (parts[2] == null || parts[2].trim().isEmpty())) {
            return false;
        }

        return true;
    }
}
