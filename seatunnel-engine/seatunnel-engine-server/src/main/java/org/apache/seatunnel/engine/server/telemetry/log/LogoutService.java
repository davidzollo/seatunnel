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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.apache.seatunnel.engine.common.utils.LogUtil;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
public class LogoutService {

    private final SeaTunnelServer seaTunnelServer;
    private String logDir;

    public LogoutService(SeaTunnelServer seaTunnelServer) {
        this.seaTunnelServer = seaTunnelServer;
        initLogDir();
    }

    private void initLogDir() {
        try {
            this.logDir = LogUtil.getLogPath();
        } catch (Exception e) {
            log.warn("Failed to get log path from LogUtil, using default 'logs' directory", e);
            this.logDir = "logs";
        }
    }

    public byte[] packageJobLogs(Long jobId) throws IOException {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipOutputStream zipOut = new ZipOutputStream(baos)) {
            addLogFilesToZip(zipOut, jobId);

            zipOut.finish();
            byte[] zipBytes = baos.toByteArray();

            log.info("Job {} logs packaged, size: {} bytes", jobId, zipBytes.length);
            return zipBytes;
        }
    }

    public byte[] packageZetaLogs() throws IOException {
        return packageZetaLogs(LocalDate.now());
    }

    public byte[] packageZetaLogs(LocalDate date) throws IOException {
        return packageZetaLogs(date, null);
    }

    public byte[] packageZetaLogs(LocalDate date, String host) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipOutputStream zipOut = new ZipOutputStream(baos)) {
            addZetaLogFilesToZip(zipOut, date);

            zipOut.finish();
            byte[] zipBytes = baos.toByteArray();

            log.info("Zeta logs for date {} packaged, size: {} bytes", date, zipBytes.length);
            return zipBytes;
        }
    }

    private void addLogFilesToZip(ZipOutputStream zipOut, Long jobId) throws IOException {
        try {
            File logDirFile = new File(logDir);
            if (logDirFile.exists() && logDirFile.isDirectory()) {
                File[] logFiles =
                        logDirFile.listFiles((dir, name) -> name.contains(jobId.toString()));
                if (logFiles != null) {
                    for (File logFile : logFiles) {
                        addFileToZip(
                                zipOut, logFile.toPath(), "job_" + jobId + "/" + logFile.getName());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to add log files for jobId {}: {}", jobId, e.getMessage(), e);
            throw new IOException("Failed to package logs for jobId " + jobId, e);
        }
    }

    private void addZetaLogFilesToZip(ZipOutputStream zipOut, LocalDate date) throws IOException {
        File logDirFile = new File(logDir);
        if (logDirFile.exists() && logDirFile.isDirectory()) {
            addDirectoryToZip(zipOut, logDirFile, "", date);
        }
    }

    private void addDirectoryToZip(
            ZipOutputStream zipOut, File dir, String basePath, LocalDate date) throws IOException {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                String filePath =
                        basePath.isEmpty() ? file.getName() : basePath + "/" + file.getName();

                if (file.isDirectory()) {
                    addDirectoryToZip(zipOut, file, filePath, date);
                } else {
                    if (isFileFromDate(file, date)) {
                        addFileToZip(zipOut, file.toPath(), filePath);
                    }
                }
            }
        }
    }

    private boolean isFileFromDate(File file, LocalDate date) {
        try {
            return LogUtil.isLogFileForDate(file.getName(), date);
        } catch (Exception e) {
            log.warn("Failed to check file date for: {}", file.getName(), e);
            return true;
        }
    }

    private void addFileToZip(ZipOutputStream zipOut, Path filePath, String entryName)
            throws IOException {
        ZipEntry zipEntry = new ZipEntry(entryName);
        zipOut.putNextEntry(zipEntry);

        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zipOut.write(buffer, 0, length);
            }
        }

        zipOut.closeEntry();
    }
}
