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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.S3TestConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file.S3FileWriter;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test helper. */
@Slf4j
public class SnowflakeFileIntegrationTest {

    private S3Client s3Client;
    private S3FileWriter s3FileWriter;
    private String testKeyPrefix;
    private SeaTunnelRowType rowType;

    @BeforeEach
    public void setUp() {
        // Test detail.
        Assumptions.assumeTrue(S3TestConfig.isS3ConfigValid(), "test message");

        try {
            // Test detail.
            AwsBasicCredentials awsCredentials =
                    AwsBasicCredentials.create(S3TestConfig.ACCESS_KEY, S3TestConfig.SECRET_KEY);

            s3Client =
                    S3Client.builder()
                            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                            .region(Region.of(S3TestConfig.S3_REGION))
                            .endpointOverride(URI.create("https://" + S3TestConfig.S3_ENDPOINT))
                            .build();

            // Test detail.
            testKeyPrefix = "seatunnel-integration-test/" + UUID.randomUUID().toString() + "/";

            // Test detail.
            rowType =
                    new SeaTunnelRowType(
                            new String[] {"id", "name", "age", "salary", "create_time"},
                            new BasicType[] {
                                BasicType.LONG_TYPE,
                                BasicType.STRING_TYPE,
                                BasicType.INT_TYPE,
                                BasicType.DOUBLE_TYPE,
                                BasicType.STRING_TYPE
                            });

            // Test detail.
            SnowflakeFileConfig config = new SnowflakeFileConfig(S3TestConfig.createS3TestConfig());
            s3FileWriter = new S3FileWriter(s3Client, config, rowType);

            log.info("test message");
            log.info("test message", S3TestConfig.S3_BUCKET);
            log.info("test message", testKeyPrefix);

        } catch (Exception e) {
            log.error("test message", e);
            Assumptions.assumeTrue(false, "test message" + e.getMessage());
        }
    }

    @Test
    public void testCompleteDataFlow() throws IOException {
        log.info("test message");

        try {
            // Test detail.
            writeTestData();

            // Test detail.
            s3FileWriter.flushAll();

            // Test detail.
            verifyFileUpload();

            // Test detail.
            verifyDataContent();

            log.info("test message");

        } catch (Exception e) {
            log.error("test message", e);
            throw e;
        } finally {
            // Test detail.
            cleanupTestFiles();
        }
    }

    @Test
    public void testMultiplePartitions() throws IOException {
        log.info("test message");

        try {
            // Test detail.
            for (int partition = 0; partition < 3; partition++) {
                String partitionId = "partition-" + partition;

                for (int i = 0; i < 5; i++) {
                    SeaTunnelRow row = new SeaTunnelRow(5);
                    row.setField(0, (long) (partition * 100 + i));
                    row.setField(1, "User-" + partition + "-" + i);
                    row.setField(2, 20 + i);
                    row.setField(3, 5000.0 + i * 100);
                    row.setField(4, "2024-01-" + String.format("%02d", i + 1));

                    s3FileWriter.writeRow(row, partitionId);
                }
            }

            // Test detail.
            s3FileWriter.flushAll();

            // Test detail.
            for (int partition = 0; partition < 3; partition++) {
                String partitionId = "partition-" + partition;
                List<String> uploadedFiles = s3FileWriter.getUploadedFiles(partitionId);

                assertFalse(uploadedFiles.isEmpty(), "test message" + partitionId + "test message");

                log.info("test message", partitionId, uploadedFiles.size());
            }

            log.info("test message");

        } catch (Exception e) {
            log.error("test message", e);
            throw e;
        } finally {
            cleanupTestFiles();
        }
    }

    @Test
    public void testLargeDataVolume() throws IOException {
        log.info("test message");

        try {
            // Test detail.
            int rowCount = 1000;
            String partitionId = "large-data-partition";

            log.info("test message", rowCount);

            for (int i = 0; i < rowCount; i++) {
                SeaTunnelRow row = new SeaTunnelRow(5);
                row.setField(0, (long) i);
                row.setField(1, "LargeDataUser" + i);
                row.setField(2, 25 + (i % 50));
                row.setField(3, 6000.0 + (i % 1000));
                row.setField(4, "2024-01-" + String.format("%02d", (i % 30) + 1));

                s3FileWriter.writeRow(row, partitionId);

                if (i % 100 == 0) {
                    log.info("test message", i);
                }
            }

            // Test detail.
            s3FileWriter.flushAll();

            // Test detail.
            List<String> uploadedFiles = s3FileWriter.getUploadedFiles(partitionId);

            assertFalse(uploadedFiles.isEmpty(), "test message");
            log.info("test message", uploadedFiles.size());

        } catch (Exception e) {
            log.error("test message", e);
            throw e;
        } finally {
            cleanupTestFiles();
        }
    }

    @Test
    public void testSpecialCharacters() throws IOException {
        log.info("test message");

        try {
            String partitionId = "special-chars-partition";

            // Test detail.
            String[] specialNames = {
                "John,Smith", // Test detail.
                "Mary\"O'Brien\"", // Test detail.
                "Li\"Hua", // Test detail.
                "test message", // Test detail.
                "Smith\\John", // Test detail.
                "O'Connor", // Test detail.
                "Test\nNewline", // Test detail.
                "Tab\tCharacter", // Test detail.
                "Normal Name" // Test detail.
            };

            for (int i = 0; i < specialNames.length; i++) {
                SeaTunnelRow row = new SeaTunnelRow(5);
                row.setField(0, (long) i);
                row.setField(1, specialNames[i]);
                row.setField(2, 25 + i);
                row.setField(3, 5000.0 + i * 100);
                row.setField(4, "2024-01-" + String.format("%02d", i + 1));

                s3FileWriter.writeRow(row, partitionId);
            }

            // Test detail.
            s3FileWriter.flushAll();

            List<String> uploadedFiles = s3FileWriter.getUploadedFiles(partitionId);
            assertFalse(uploadedFiles.isEmpty(), "test message");

            log.info("test message", specialNames.length);

        } catch (Exception e) {
            log.error("test message", e);
            throw e;
        } finally {
            cleanupTestFiles();
        }
    }

    @Test
    public void testNullValues() throws IOException {
        log.info("test message");

        try {
            String partitionId = "null-values-partition";

            // Test detail.
            for (int i = 0; i < 10; i++) {
                SeaTunnelRow row = new SeaTunnelRow(5);
                row.setField(0, (long) i);
                row.setField(1, i % 3 == 0 ? null : "User" + i); // Test detail.
                row.setField(2, i % 4 == 0 ? null : 25 + i); // Test detail.
                row.setField(3, 5000.0 + i * 100);
                row.setField(4, "2024-01-" + String.format("%02d", i + 1));

                s3FileWriter.writeRow(row, partitionId);
            }

            // Test detail.
            s3FileWriter.flushAll();

            List<String> uploadedFiles = s3FileWriter.getUploadedFiles(partitionId);
            assertFalse(uploadedFiles.isEmpty(), "test message");

            log.info("test message");

        } catch (Exception e) {
            log.error("test message", e);
            throw e;
        } finally {
            cleanupTestFiles();
        }
    }

    private void writeTestData() throws IOException {
        log.info("test message");

        // Test detail.
        List<SeaTunnelRow> testData =
                Arrays.asList(
                        createRow(1L, "Alice", 25, 5000.0, "2024-01-01"),
                        createRow(2L, "Bob", 30, 6000.0, "2024-01-02"),
                        createRow(3L, "Charlie", 35, 7000.0, "2024-01-03"),
                        createRow(4L, "Diana", 28, 5500.0, "2024-01-04"),
                        createRow(5L, "Eve", 32, 6500.0, "2024-01-05"));

        String partitionId = "test-partition";
        for (SeaTunnelRow row : testData) {
            s3FileWriter.writeRow(row, partitionId);
        }

        log.info("test message", testData.size());
    }

    private SeaTunnelRow createRow(
            long id, String name, int age, double salary, String createTime) {
        SeaTunnelRow row = new SeaTunnelRow(5);
        row.setField(0, id);
        row.setField(1, name);
        row.setField(2, age);
        row.setField(3, salary);
        row.setField(4, createTime);
        return row;
    }

    private void verifyFileUpload() {
        log.info("test message");

        List<String> uploadedFiles = s3FileWriter.getUploadedFiles("test-partition");
        assertFalse(uploadedFiles.isEmpty(), "test message");

        log.info("test message", uploadedFiles.size());
        for (String file : uploadedFiles) {
            log.info("test message", file);
        }
    }

    private void verifyDataContent() {
        log.info("test message");

        // Test detail.
        // Test detail.
        List<String> uploadedFiles = s3FileWriter.getUploadedFiles("test-partition");

        assertTrue(uploadedFiles.size() >= 1, "test message");
        assertTrue(
                uploadedFiles.get(0).contains(S3TestConfig.S3_BUCKET.replace("s3a://", "")),
                "test message");

        log.info("test message");
    }

    private void cleanupTestFiles() {
        log.info("test message");

        try {
            // Test detail.
            ConcurrentMap<String, List<String>> allPartitionFiles =
                    s3FileWriter.getAllUploadedFiles();

            // Test detail.
            List<String> allFiles = new ArrayList<>();
            allPartitionFiles.values().forEach(allFiles::addAll);

            if (!allFiles.isEmpty()) {
                // Test detail.
                s3FileWriter.cleanupFiles(allFiles);
                log.info("test message", allFiles.size());
            } else {
                log.info("test message");
            }
        } catch (Exception e) {
            log.warn("test message", e.getMessage());
            // Test detail.
        }
    }
}
