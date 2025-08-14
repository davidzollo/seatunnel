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

package org.apache.seatunnel.engine.server.service;

import org.apache.seatunnel.engine.common.utils.LogUtil;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LogUtilTest {

    @Test
    void testGetLogPath() {
        try {
            String logPath = LogUtil.getLogPath();
            assertNotNull(logPath);
            assertFalse(logPath.isEmpty());
            System.out.println("Log path: " + logPath);
            assertNotEquals("${file_path}", logPath);
        } catch (Exception e) {
            fail("Failed to get log path: " + e.getMessage());
        }
    }

    @Test
    void testGetLogFileName() {
        try {
            String fileName = LogUtil.getLogFileName();
            assertNotNull(fileName);
            assertFalse(fileName.isEmpty());
            System.out.println("Log file name: " + fileName);
        } catch (Exception e) {
            fail("Failed to get log file name: " + e.getMessage());
        }
    }

    @Test
    void testGetLogFilePattern() {
        try {
            String pattern = LogUtil.getLogFilePattern();
            assertNotNull(pattern);
            assertFalse(pattern.isEmpty());
            System.out.println("Log file pattern: " + pattern);
        } catch (Exception e) {
            fail("Failed to get log file pattern: " + e.getMessage());
        }
    }

    @Test
    void testIsLogFileForDate() {
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        LocalDate today = LocalDate.now();

        assertTrue(LogUtil.isLogFileForDate("seatunnel.log", today));
        assertFalse(LogUtil.isLogFileForDate("seatunnel.log", testDate));

        assertTrue(LogUtil.isLogFileForDate("seatunnel.log.2023-12-01-1", testDate));
        assertTrue(LogUtil.isLogFileForDate("seatunnel.log.2023-12-01-2", testDate));

        assertFalse(LogUtil.isLogFileForDate("seatunnel.log.2023-12-02-1", testDate));
        assertFalse(LogUtil.isLogFileForDate("seatunnel.log.2023-11-30-1", testDate));

        assertFalse(LogUtil.isLogFileForDate("other.log", testDate));
        assertFalse(LogUtil.isLogFileForDate("seatunnel_job_123.log", testDate));
    }
}
