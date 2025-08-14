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

import org.apache.seatunnel.engine.server.telemetry.log.LogoutService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogServiceTest {

    @TempDir Path tempDir;

    private LogoutService logOutService;

    @BeforeEach
    void setUp() {
        logOutService = new LogoutService(null);
    }

    @Test
    void testPackageJobLogs() throws IOException {
        Path logsDir = tempDir.resolve("logs");
        Files.createDirectories(logsDir);

        Path mainLogFile = logsDir.resolve("seatunnel.log");
        Files.write(mainLogFile, "Test main log content".getBytes());

        Path rollingLogFile = logsDir.resolve("seatunnel.log.2023-12-01-1");
        Files.write(rollingLogFile, "Test rolling log content".getBytes());

        byte[] zipBytes = logOutService.packageJobLogs(123L);

        assertNotNull(zipBytes);
        assertTrue(zipBytes.length > 0);
        // delete the log files after test
        if (Files.exists(mainLogFile)) {
            Files.delete(mainLogFile);
        }
        if (Files.exists(rollingLogFile)) {
            Files.delete(rollingLogFile);
        }
        if (Files.exists(logsDir)) {
            Files.delete(logsDir);
        }
    }

    @Test
    void testPackageAllLogs() throws IOException {
        Path logsDir = tempDir.resolve("logs");
        Files.createDirectories(logsDir);

        Path mainLogFile = logsDir.resolve("seatunnel.log");
        Path rollingLogFile = logsDir.resolve("seatunnel.log.2023-12-01-1");
        Files.write(mainLogFile, "Test main log content".getBytes());
        Files.write(rollingLogFile, "Test rolling log content".getBytes());

        byte[] zipBytes = logOutService.packageZetaLogs();

        assertNotNull(zipBytes);
        assertTrue(zipBytes.length > 0);

        // delete the log files after test
        if (Files.exists(mainLogFile)) {
            Files.delete(mainLogFile);
        }
        if (Files.exists(rollingLogFile)) {
            Files.delete(rollingLogFile);
        }
        if (Files.exists(logsDir)) {
            Files.delete(logsDir);
        }
    }

    @Test
    void testPackageZetaLogsWithDate() throws IOException {
        Path logsDir = tempDir.resolve("logs");
        Files.createDirectories(logsDir);

        Path mainLogFile = logsDir.resolve("seatunnel.log");
        Path rollingLogFile = logsDir.resolve("seatunnel.log.2023-12-01-1");
        Files.write(mainLogFile, "Test main log content".getBytes());
        Files.write(rollingLogFile, "Test rolling log content".getBytes());

        LocalDate testDate = LocalDate.now();
        byte[] zipBytes = logOutService.packageZetaLogs(testDate);

        assertNotNull(zipBytes);
        assertTrue(zipBytes.length > 0);
        // delete the log files after test
        if (Files.exists(mainLogFile)) {
            Files.delete(mainLogFile);
        }
        if (Files.exists(rollingLogFile)) {
            Files.delete(rollingLogFile);
        }
        if (Files.exists(logsDir)) {
            Files.delete(logsDir);
        }
    }
}
