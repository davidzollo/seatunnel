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

package org.apache.seatunnel.common.utils;

import org.slf4j.Logger;

import java.time.format.DateTimeFormatter;

/**
 * Common logging utility class that provides standardized logging templates for split and data
 * reading processes, unified log format management to avoid code duplication
 */
public class LoggingUtils {

    private static final String SEPARATOR_CHAR = "*";
    private static final int MIN_SEPARATOR_LENGTH = 40;
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Generate separator line with adaptive length
     *
     * @param title Title to calculate separator length
     * @return Separator string with appropriate length
     */
    private static String generateSeparator(String title) {
        int titleLength = title != null ? title.length() : 0;
        int separatorLength = Math.max(MIN_SEPARATOR_LENGTH, titleLength + 20);
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < separatorLength; i++) {
            separator.append(SEPARATOR_CHAR);
        }
        return separator.toString();
    }

    /**
     * Log the start of process
     *
     * @param logger Logger instance
     * @param processName Process name, e.g. "Reading data process"
     */
    public static void logStart(Logger logger, String processName) {
        String separator = generateSeparator(processName);
        logger.info(separator);
        logger.info("Starting {} process", processName.toLowerCase());
        logger.info(separator);
    }

    /**
     * Log the end of process
     *
     * @param logger Logger instance
     * @param processName Process name, e.g. "Reading data process"
     */
    public static void logEnd(Logger logger, String processName) {
        String separator = generateSeparator(processName);
        logger.info(separator);
        logger.info("{} process completed", processName);
        logger.info(separator);
    }

    /**
     * Get separator string
     *
     * @return Separator string
     */
    public static String getSeparator() {
        return generateSeparator(null);
    }
}
