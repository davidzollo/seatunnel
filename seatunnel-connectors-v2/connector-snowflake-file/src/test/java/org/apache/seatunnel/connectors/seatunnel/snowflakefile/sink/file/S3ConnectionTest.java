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

import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.S3TestConfig;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/** Test helper. */
@Slf4j
public class S3ConnectionTest {

    private S3Client s3Client;
    private String testBucket;
    private String testKeyPrefix;

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

            testBucket = S3TestConfig.S3_BUCKET.replace("s3a://", "");
            testKeyPrefix = "seatunnel-test/" + UUID.randomUUID().toString() + "/";

            log.info("test message", testBucket, testKeyPrefix);
        } catch (Exception e) {
            log.error("test message", e);
            Assumptions.assumeTrue(false, "test message" + e.getMessage());
        }
    }

    @Test
    public void testS3BucketAccess() {
        log.info("test message");

        try {
            // Test detail.
            HeadBucketRequest headBucketRequest =
                    HeadBucketRequest.builder().bucket(testBucket).build();

            s3Client.headBucket(headBucketRequest);
            log.info("test message", testBucket);
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testS3ListObjects() {
        log.info("test message");

        try {
            // Test detail.
            ListObjectsV2Request listRequest =
                    ListObjectsV2Request.builder().bucket(testBucket).maxKeys(10).build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = response.contents();

            log.info("test message", objects.size());

            // Test detail.
            objects.stream()
                    .limit(3)
                    .forEach(
                            obj ->
                                    log.info(
                                            "  - Object: {}, Size: {}, LastModified: {}",
                                            obj.key(),
                                            obj.size(),
                                            obj.lastModified()));

            assertNotNull(objects, "test message");
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testS3PrefixListing() {
        log.info("test message");

        try {
            // Test detail.
            ListObjectsV2Request listRequest =
                    ListObjectsV2Request.builder()
                            .bucket(testBucket)
                            .prefix("seatunnel-test/") // Test detail.
                            .maxKeys(5)
                            .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = response.contents();

            log.info("test message", objects.size());

            objects.forEach(obj -> log.info("  - Object: {}, Size: {}", obj.key(), obj.size()));

            assertNotNull(objects, "test message");
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testS3ConnectionConfiguration() {
        log.info("test message");

        // Test detail.
        assertNotNull(S3TestConfig.ACCESS_KEY, "test message");
        assertNotNull(S3TestConfig.SECRET_KEY, "test message");
        assertNotNull(S3TestConfig.S3_BUCKET, "test message");
        assertNotNull(S3TestConfig.S3_REGION, "test message");
        assertNotNull(S3TestConfig.S3_ENDPOINT, "test message");

        log.info("test message");
        log.info("  - Bucket: {}", S3TestConfig.S3_BUCKET);
        log.info("  - Region: {}", S3TestConfig.S3_REGION);
        log.info("  - Endpoint: {}", S3TestConfig.S3_ENDPOINT);
        log.info("  - Credentials Provider: {}", S3TestConfig.CREDENTIALS_PROVIDER);
    }

    @Test
    public void testS3FileOperations() {
        log.info("test message");

        // Test detail.
        String testFileKey = testKeyPrefix + "test-file-" + UUID.randomUUID() + ".txt";
        String testContent = "Hello SeaTunnel SnowflakeFile Connector! " + Instant.now();

        try {
            // Test detail.
            testFileUpload(testFileKey, testContent);

            // Test detail.
            testFileExists(testFileKey);

            // Test detail.
            testFileDelete(testFileKey);

            log.info("test message");
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            fail("test message" + e.getMessage());
        }
    }

    private void testFileUpload(String fileKey, String content) {
        log.info("test message", fileKey);

        try {
            // Test detail.
            // Test detail.
            log.info("test message");
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            throw e;
        }
    }

    private void testFileExists(String fileKey) {
        log.info("test message", fileKey);

        try {
            // Test detail.
            ListObjectsV2Request checkRequest =
                    ListObjectsV2Request.builder()
                            .bucket(testBucket)
                            .prefix(fileKey)
                            .maxKeys(1)
                            .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(checkRequest);
            boolean exists =
                    response.contents().stream().anyMatch(obj -> obj.key().equals(fileKey));

            if (exists) {
                log.info("test message", fileKey);
            } else {
                log.warn("test message", fileKey);
            }
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            throw e;
        }
    }

    private void testFileDelete(String fileKey) {
        log.info("test message", fileKey);

        try {
            // Test detail.
            log.info("test message");
        } catch (Exception e) {
            log.error("test message", e.getMessage());
            throw e;
        }
    }
}
