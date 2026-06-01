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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class LocalFileWriter implements StagingFileWriter {

    private final Path baseDir;
    private final String fieldDelimiter;
    private final String recordDelimiter;
    private final String fileExtension;
    private final long maxFileSize;
    private final int bufferSize;
    private final SeaTunnelRowType rowType;
    private final ConcurrentMap<String, LocalFileState> fileStates;

    private static class LocalFileState {
        private final String partitionId;
        private final List<String> uploadedFiles;
        private long currentSize;
        private int fileIndex;
        private Path currentFile;
        private BufferedWriter writer;

        private LocalFileState(String partitionId) {
            this.partitionId = partitionId;
            this.uploadedFiles = new ArrayList<>();
        }
    }

    public LocalFileWriter(SnowflakeFileConfig config, SeaTunnelRowType rowType, String writerId) {
        try {
            this.baseDir = Paths.get(config.getLocalTempDir()).resolve(writerId);
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local temp directory", e);
        }
        this.fieldDelimiter = config.getFieldDelimiter();
        this.recordDelimiter = config.getRecordDelimiter();
        this.fileExtension = config.getFileExtension();
        this.maxFileSize = config.getMaxFileSize();
        this.bufferSize =
                Math.max(config.getBufferSize(), SnowflakeFileConfig.BUFFER_SIZE.defaultValue());
        this.rowType = rowType;
        this.fileStates = new ConcurrentHashMap<>();
    }

    @Override
    public void writeRow(SeaTunnelRow row, String partitionId) throws IOException {
        LocalFileState fileState = fileStates.computeIfAbsent(partitionId, LocalFileState::new);
        String csvLine = convertRowToCSV(row);
        byte[] csvBytes = csvLine.getBytes(StandardCharsets.UTF_8);

        if (fileState.writer == null || fileState.currentSize + csvBytes.length > maxFileSize) {
            rotateFile(fileState);
        }

        fileState.writer.write(csvLine);
        fileState.currentSize += csvBytes.length;
    }

    @Override
    public List<String> getUploadedFiles(String partitionId) {
        LocalFileState fileState = fileStates.get(partitionId);
        return fileState != null ? new ArrayList<>(fileState.uploadedFiles) : new ArrayList<>();
    }

    @Override
    public ConcurrentMap<String, List<String>> getAllUploadedFiles() {
        ConcurrentMap<String, List<String>> result = new ConcurrentHashMap<>();
        fileStates.forEach(
                (partitionId, fileState) ->
                        result.put(partitionId, new ArrayList<>(fileState.uploadedFiles)));
        return result;
    }

    @Override
    public void clearUploadedFiles() {
        fileStates.values().forEach(fileState -> fileState.uploadedFiles.clear());
    }

    @Override
    public void flushAll() throws IOException {
        for (LocalFileState fileState : fileStates.values()) {
            closeCurrentFile(fileState);
        }
    }

    @Override
    public void cleanupFiles(List<String> fileUrls) {
        for (String fileUrl : fileUrls) {
            try {
                Files.deleteIfExists(Paths.get(fileUrl));
            } catch (IOException e) {
                log.warn("Failed to delete local staging file: {}", fileUrl, e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        flushAll();
        fileStates.clear();
    }

    private String convertRowToCSV(SeaTunnelRow row) {
        StringBuilder csvBuilder = new StringBuilder();

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (i > 0) {
                csvBuilder.append(fieldDelimiter);
            }

            Object value = row.getField(i);
            if (value != null) {
                String stringValue = value.toString();
                if (stringValue.contains(fieldDelimiter)
                        || stringValue.contains("\"")
                        || stringValue.contains("\n")) {
                    csvBuilder.append("\"").append(stringValue.replace("\"", "\"\"")).append("\"");
                } else {
                    csvBuilder.append(stringValue);
                }
            }
        }

        csvBuilder.append(recordDelimiter);
        return csvBuilder.toString();
    }

    private void rotateFile(LocalFileState fileState) throws IOException {
        closeCurrentFile(fileState);

        String fileName = generateFileName(fileState.partitionId, fileState.fileIndex);
        fileState.currentFile = baseDir.resolve(fileName);
        fileState.writer =
                new BufferedWriter(
                        Files.newBufferedWriter(
                                fileState.currentFile,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING),
                        bufferSize);
        fileState.uploadedFiles.add(fileState.currentFile.toAbsolutePath().toString());
        fileState.currentSize = 0;
        fileState.fileIndex++;
    }

    private void closeCurrentFile(LocalFileState fileState) throws IOException {
        if (fileState.writer != null) {
            fileState.writer.flush();
            fileState.writer.close();
            fileState.writer = null;
        }
    }

    private String generateFileName(String partitionId, int fileIndex) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("part-%s-%s-%d%s", partitionId, uuid, fileIndex, fileExtension);
    }
}
