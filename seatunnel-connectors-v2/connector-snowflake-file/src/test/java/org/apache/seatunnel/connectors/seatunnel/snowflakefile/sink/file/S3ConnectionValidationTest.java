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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
/** Test helper. */
public class S3ConnectionValidationTest {

    private S3Client s3Client;
    private String testBucket;
    private String testKey;

    @BeforeEach
    public void setUp() {
        Assumptions.assumeTrue(
                S3TestConfig.isS3ConfigValid(),
                "Set SNOWFLAKE_FILE_TEST_S3_* env vars to run real S3 validation");
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
            testKey = "seatunnel-validation-test/" + UUID.randomUUID().toString() + ".txt";

            log.info("test message");
            log.info("test message", testBucket);
            log.info("test message", testKey);
            log.info("test message", S3TestConfig.S3_REGION);
            log.info("test message", S3TestConfig.S3_ENDPOINT);

        } catch (Exception e) {
            log.error("test message", e);
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testListBuckets() {
        log.info("test message");

        try {
            ListBucketsResponse response = s3Client.listBuckets();
            List<Bucket> buckets = response.buckets();

            log.info("test message", buckets.size());
            buckets.forEach(
                    bucket -> log.info("test message", bucket.name(), bucket.creationDate()));

            // Test detail.
            boolean targetBucketExists =
                    buckets.stream().anyMatch(bucket -> bucket.name().equals(testBucket));

            if (targetBucketExists) {
                log.info("test message", testBucket);
            } else {
                log.error("test message", testBucket);
                log.info("test message", buckets.stream().map(Bucket::name).toArray());
            }

            assertTrue(targetBucketExists, "test message");

        } catch (Exception e) {
            log.error("test message", e.getMessage(), e);
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testBucketAccess() {
        log.info("test message");

        try {
            HeadBucketRequest headBucketRequest =
                    HeadBucketRequest.builder().bucket(testBucket).build();

            s3Client.headBucket(headBucketRequest);
            log.info("test message", testBucket);

        } catch (Exception e) {
            log.error("test message", testBucket, e.getMessage(), e);
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testFileUpload() {
        log.info("test message");

        String testContent =
                "Hello from SeaTunnel SnowflakeFile Connector!\nTest time: " + Instant.now();

        try {
            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .bucket(testBucket)
                            .key(testKey)
                            .contentType("text/plain")
                            .build();

            PutObjectResponse response =
                    s3Client.putObject(putObjectRequest, RequestBody.fromString(testContent));

            log.info("test message");
            log.info("test message", testBucket, testKey);
            log.info("test message", testContent.length());
            log.info("  - ETag: {}", response.eTag());
            log.info("test message", response.versionId() != null ? response.versionId() : "N/A");

            assertNotNull(response.eTag(), "test message");

        } catch (Exception e) {
            log.error("test message", e.getMessage(), e);
            fail("test message" + e.getMessage());
        }
    }

    @Test
    public void testBucketLocation() {
        log.info("test message");

        try {
            // Test detail.
            software.amazon.awssdk.services.s3.model.GetBucketLocationResponse locationResponse =
                    s3Client.getBucketLocation(
                            software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
                                    .builder()
                                    .bucket(testBucket)
                                    .build());

            String bucketLocation = locationResponse.locationConstraint().toString();

            log.info("test message");
            log.info("test message", testBucket);
            log.info("test message", bucketLocation);
            log.info("test message", S3TestConfig.S3_REGION);

            // Test detail.
            if (bucketLocation.contains(S3TestConfig.S3_REGION)) {
                log.info("test message");
            } else {
                log.warn("test message", bucketLocation, S3TestConfig.S3_REGION);
            }

        } catch (Exception e) {
            log.error("test message", e.getMessage(), e);
            // Test detail.
        }
    }

    @Test
    public void testEndpointConnectivity() {
        log.info("test message");

        try {
            // Test detail.
            s3Client.listBuckets();
            log.info("test message", S3TestConfig.S3_ENDPOINT);

        } catch (Exception e) {
            log.error("test message", S3TestConfig.S3_ENDPOINT, e.getMessage(), e);
            fail("test message" + e.getMessage());
        }
    }
}
