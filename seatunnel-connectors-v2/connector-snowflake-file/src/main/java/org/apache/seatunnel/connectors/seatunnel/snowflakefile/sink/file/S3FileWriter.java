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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class S3FileWriter implements StagingFileWriter {

    private final S3Client s3Client;
    private final String bucketName;
    private final String keyPrefix;
    private final String fieldDelimiter;
    private final String recordDelimiter;
    private final String fileExtension;
    private final long maxFileSize;
    private final int bufferSize;
    private final SeaTunnelRowType rowType;
    private final List<String> fieldNames;

    // File state per partition
    private final ConcurrentMap<String, S3FileState> fileStates;

    private static class S3FileState {
        private final String partitionId;
        private final ByteArrayOutputStream buffer;
        private final List<String> uploadedFiles;
        private long currentSize;
        private int fileIndex;

        public S3FileState(String partitionId, int bufferSize) {
            this.partitionId = partitionId;
            this.buffer = new ByteArrayOutputStream(bufferSize);
            this.uploadedFiles = new ArrayList<>();
            this.currentSize = 0;
            this.fileIndex = 0;
        }
    }

    public S3FileWriter(S3Client s3Client, SnowflakeFileConfig config, SeaTunnelRowType rowType) {
        this.s3Client = s3Client;
        this.bucketName = cleanBucketName(config.getS3Bucket());
        this.keyPrefix = config.getS3KeyPrefix();
        this.fieldDelimiter = config.getFieldDelimiter();
        this.recordDelimiter = config.getRecordDelimiter();
        this.fileExtension = config.getFileExtension();
        this.maxFileSize = config.getMaxFileSize();
        this.bufferSize =
                Math.max(config.getBufferSize(), SnowflakeFileConfig.BUFFER_SIZE.defaultValue());
        this.rowType = rowType;
        this.fieldNames =
                java.util.Arrays.stream(rowType.getFieldNames()).collect(Collectors.toList());
        this.fileStates = new ConcurrentHashMap<>();
    }

    /** Clean the S3 bucket name by removing s3:// or s3a:// prefixes */
    private String cleanBucketName(String bucketName) {
        if (bucketName == null) {
            return null;
        }
        // Remove s3:// or s3a:// prefixes
        return bucketName.trim().replaceFirst("^s3[an]?://", "").replaceAll("/+$", "");
    }

    /** Write one row to S3 */
    @Override
    public void writeRow(SeaTunnelRow row, String partitionId) throws IOException {
        S3FileState fileState =
                fileStates.computeIfAbsent(partitionId, key -> new S3FileState(key, bufferSize));

        // Convert row data to CSV
        String csvLine = convertRowToCSV(row);
        byte[] csvBytes = csvLine.getBytes(StandardCharsets.UTF_8);

        // Check whether a new file should be rolled
        if (fileState.currentSize + csvBytes.length > maxFileSize) {
            log.info(
                    "Flushing file for partition {} (current size: {}, max: {})",
                    partitionId,
                    fileState.currentSize,
                    maxFileSize);
            flushBufferToS3(fileState);
        }

        // Write to the buffer
        fileState.buffer.write(csvBytes);
        fileState.currentSize += csvBytes.length;
    }

    /** Convert SeaTunnelRow to a CSV row */
    private String convertRowToCSV(SeaTunnelRow row) {
        StringBuilder csvBuilder = new StringBuilder();

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (i > 0) {
                csvBuilder.append(fieldDelimiter);
            }

            Object value = row.getField(i);
            if (value != null) {
                String stringValue = value.toString();
                // Handle fields containing delimiters or quotes
                if (stringValue.contains(fieldDelimiter)
                        || stringValue.contains("\"")
                        || stringValue.contains("\n")) {
                    csvBuilder.append("\"").append(stringValue.replace("\"", "\"\"")).append("\"");
                } else {
                    csvBuilder.append(stringValue);
                }
            }
            // Leave null values empty
        }

        csvBuilder.append(recordDelimiter);
        return csvBuilder.toString();
    }

    /** Flush the buffer to S3 */
    private void flushBufferToS3(S3FileState fileState) throws IOException {
        if (fileState.currentSize == 0) {
            return;
        }

        String fileName = generateFileName(fileState.partitionId, fileState.fileIndex);
        String s3Key = keyPrefix + fileName;

        try {
            byte[] data = fileState.buffer.toByteArray();

            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Key)
                            .contentLength((long) data.length)
                            .contentType("text/csv")
                            .build();

            RequestBody requestBody = RequestBody.fromBytes(data);
            s3Client.putObject(putObjectRequest, requestBody);

            String uploadedFile = "s3://" + bucketName + "/" + s3Key;
            fileState.uploadedFiles.add(uploadedFile);

            log.info("Uploaded file to S3: {} (size: {} bytes)", uploadedFile, data.length);

            // Reset the buffer
            fileState.buffer.reset();
            fileState.currentSize = 0;
            fileState.fileIndex++;

        } catch (Exception e) {
            log.error(
                    "Failed to upload file to S3: {} (size: {} bytes)",
                    fileName,
                    fileState.currentSize,
                    e);
            throw new IOException("Failed to upload file to S3: " + fileName, e);
        }
    }

    /** Generate file name */
    private String generateFileName(String partitionId, int fileIndex) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("part-%s-%s-%d%s", partitionId, uuid, fileIndex, fileExtension);
    }

    /** Get all uploaded files */
    @Override
    public List<String> getUploadedFiles(String partitionId) {
        S3FileState fileState = fileStates.get(partitionId);
        return fileState != null ? new ArrayList<>(fileState.uploadedFiles) : new ArrayList<>();
    }

    /** Get uploaded files for all partitions */
    @Override
    public ConcurrentMap<String, List<String>> getAllUploadedFiles() {
        ConcurrentMap<String, List<String>> result = new ConcurrentHashMap<>();
        fileStates.forEach(
                (partitionId, fileState) -> {
                    result.put(partitionId, new ArrayList<>(fileState.uploadedFiles));
                });
        return result;
    }

    @Override
    public void clearUploadedFiles() {
        fileStates.values().forEach(fileState -> fileState.uploadedFiles.clear());
    }

    /** Flush all buffered data to S3 */
    @Override
    public void flushAll() throws IOException {
        for (S3FileState fileState : fileStates.values()) {
            if (fileState.currentSize > 0) {
                flushBufferToS3(fileState);
            }
        }
    }

    /** Cleanup files on S3 */
    @Override
    public void cleanupFiles(List<String> s3FileUrls) {
        for (String s3FileUrl : s3FileUrls) {
            try {
                // Parse S3 URL: s3://bucket/key
                String[] parts = s3FileUrl.replace("s3://", "").split("/", 2);
                if (parts.length == 2) {
                    String bucket = parts[0];
                    String key = parts[1];

                    DeleteObjectRequest deleteObjectRequest =
                            DeleteObjectRequest.builder().bucket(bucket).key(key).build();

                    s3Client.deleteObject(deleteObjectRequest);
                    log.info("Deleted S3 file: {}", s3FileUrl);
                }
            } catch (Exception e) {
                log.warn("Failed to delete S3 file: {}", s3FileUrl, e);
            }
        }
    }

    /** Close writer */
    @Override
    public void close() throws IOException {
        try {
            flushAll();
        } finally {
            fileStates.clear();
        }
    }
}
