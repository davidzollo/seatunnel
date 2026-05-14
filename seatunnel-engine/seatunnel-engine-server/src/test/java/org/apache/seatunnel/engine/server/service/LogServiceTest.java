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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogServiceTest {

    @TempDir Path tempDir;

    private LogoutService logOutService;
    private Path logsDir;

    @BeforeEach
    void setUp() throws Exception {
        logOutService = new LogoutService(null);
        logsDir = tempDir.resolve("logs");
        Files.createDirectories(logsDir);
        setLogDir(logsDir);
    }

    @Test
    void testPackageJobLogsIncludesFlatAndNestedJobLogFiles() throws IOException {
        Files.write(
                logsDir.resolve("job-123.log"), "flat job log".getBytes(StandardCharsets.UTF_8));
        Files.write(
                logsDir.resolve("job-123.log.2026-05-12-1"),
                "rolled job log".getBytes(StandardCharsets.UTF_8));
        Files.write(
                logsDir.resolve("job-456.log"), "other job log".getBytes(StandardCharsets.UTF_8));

        Path nestedDir = logsDir.resolve("pod-a");
        Files.createDirectories(nestedDir);
        Files.write(
                nestedDir.resolve("job-123.log"),
                "nested job log".getBytes(StandardCharsets.UTF_8));

        Map<String, String> zipEntries = readZipEntries(logOutService.packageJobLogs(123L));

        assertEquals(3, zipEntries.size());
        assertEquals("flat job log", zipEntries.get("job_123/job-123.log"));
        assertEquals("rolled job log", zipEntries.get("job_123/job-123.log.2026-05-12-1"));
        assertEquals("nested job log", zipEntries.get("job_123/pod-a/job-123.log"));
        assertFalse(zipEntries.containsKey("job_123/job-456.log"));
    }

    @Test
    void testPackageZetaLogsIncludesCurrentDateLogs() throws IOException {
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Files.write(logsDir.resolve("seatunnel.log"), "main log".getBytes(StandardCharsets.UTF_8));
        Files.write(
                logsDir.resolve("seatunnel.log." + today + "-1"),
                "today rolled log".getBytes(StandardCharsets.UTF_8));

        Map<String, String> zipEntries = readZipEntries(logOutService.packageZetaLogs());

        assertEquals("main log", zipEntries.get("seatunnel.log"));
        assertEquals("today rolled log", zipEntries.get("seatunnel.log." + today + "-1"));
    }

    @Test
    void testPackageZetaLogsWithDateFiltersOtherDates() throws IOException {
        LocalDate targetDate = LocalDate.now().minusDays(1);
        String targetDateText = targetDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String otherDateText =
                targetDate.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        Files.write(
                logsDir.resolve("seatunnel.log." + targetDateText + "-1"),
                "target day log".getBytes(StandardCharsets.UTF_8));
        Files.write(
                logsDir.resolve("seatunnel.log." + otherDateText + "-1"),
                "other day log".getBytes(StandardCharsets.UTF_8));

        Map<String, String> zipEntries = readZipEntries(logOutService.packageZetaLogs(targetDate));

        assertEquals(1, zipEntries.size());
        assertEquals("target day log", zipEntries.get("seatunnel.log." + targetDateText + "-1"));
        assertTrue(zipEntries.containsKey("seatunnel.log." + targetDateText + "-1"));
    }

    private void setLogDir(Path logDir) throws Exception {
        Field logDirField = LogoutService.class.getDeclaredField("logDir");
        logDirField.setAccessible(true);
        logDirField.set(logOutService, logDir.toString());
    }

    private Map<String, String> readZipEntries(byte[] zipBytes) throws IOException {
        Map<String, String> zipEntries = new LinkedHashMap<>();
        try (ZipInputStream zipInputStream =
                new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = zipInputStream.read(buffer)) > 0) {
                    outputStream.write(buffer, 0, length);
                }
                zipEntries.put(
                        zipEntry.getName(), outputStream.toString(StandardCharsets.UTF_8.name()));
                zipInputStream.closeEntry();
            }
        }
        return zipEntries;
    }
}
