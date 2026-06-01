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
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;

/** Test helper. */
@Slf4j
public class S3ErrorDiagnosisTest {

    @Test
    public void diagnoseS3Connection() {
        Assumptions.assumeTrue(
                S3TestConfig.isS3ConfigValid(),
                "Set SNOWFLAKE_FILE_TEST_S3_* env vars to run real S3 diagnostics");
        log.info("test message");
        log.info("test message");
        log.info("test message", S3TestConfig.S3_BUCKET);
        log.info("test message", S3TestConfig.S3_REGION);
        log.info("test message", S3TestConfig.S3_ENDPOINT);
        log.info(
                "test message",
                S3TestConfig.ACCESS_KEY.substring(
                        0, Math.min(8, S3TestConfig.ACCESS_KEY.length())));

        // Test detail.
        String[] bucketFormats = {
            S3TestConfig.S3_BUCKET, // "s3a://wt-auto-bucket"
            "wt-auto-bucket", // Test detail.
            "s3://wt-auto-bucket", // Test detail.
            "wt-auto-bucket/", // Test detail.
            "s3a://wt-auto-bucket/" // Test detail.
        };

        for (String bucketFormat : bucketFormats) {
            testBucketFormat(bucketFormat);
        }
    }

    private void testBucketFormat(String bucketName) {
        log.info("test message", bucketName);

        try {
            // Test detail.
            String cleanBucketName = cleanBucketName(bucketName);
            log.info("test message", cleanBucketName);

            // Test detail.
            AwsBasicCredentials awsCredentials =
                    AwsBasicCredentials.create(S3TestConfig.ACCESS_KEY, S3TestConfig.SECRET_KEY);

            S3Client s3Client =
                    S3Client.builder()
                            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                            .region(Region.of(S3TestConfig.S3_REGION))
                            .endpointOverride(URI.create("https://" + S3TestConfig.S3_ENDPOINT))
                            .build();

            // Test detail.
            HeadBucketRequest headBucketRequest =
                    HeadBucketRequest.builder().bucket(cleanBucketName).build();

            log.info("test message");
            HeadBucketResponse response = s3Client.headBucket(headBucketRequest);

            log.info("test message");
            log.info("test message", response);

            s3Client.close();

        } catch (S3Exception e) {
            log.error("test message");
            log.error("test message", e.awsErrorDetails().errorCode());
            log.error("test message", e.awsErrorDetails().errorMessage());
            log.error("test message", e.statusCode());
            log.error("test message", e.requestId());
            log.error("test message", e.extendedRequestId());

            // Test detail.
            analyzeS3Error(e);

        } catch (Exception e) {
            log.error("test message");
            log.error("test message", e.getClass().getSimpleName());
            log.error("test message", e.getMessage());
            log.error("test message", e);
        }
    }

    private void analyzeS3Error(S3Exception e) {
        String errorCode = e.awsErrorDetails().errorCode();
        String errorMessage = e.awsErrorDetails().errorMessage();
        int statusCode = e.statusCode();

        log.info("test message");
        log.info("test message", statusCode);
        log.info("test message", errorCode);
        log.info("test message", errorMessage);

        if (statusCode == 400) {
            log.info("test message");
            log.info("test message");
            log.info("test message");
            log.info("test message");
            log.info("test message");
            log.info("test message");

            if (errorMessage != null) {
                if (errorMessage.contains("not valid")) {
                    log.info("test message");
                } else if (errorMessage.contains("not exist")) {
                    log.info("test message");
                } else if (errorMessage.contains("InvalidAccessKeyId")) {
                    log.info("test message");
                } else if (errorMessage.contains("SignatureDoesNotMatch")) {
                    log.info("test message");
                }
            }
        } else if (statusCode == 403) {
            log.info("test message");
        } else if (statusCode == 404) {
            log.info("test message");
        } else {
            log.info("test message", statusCode);
        }
    }

    /** Clean bucket names by removing S3 protocol prefixes and trailing slashes. */
    private String cleanBucketName(String bucketName) {
        if (bucketName == null) {
            return null;
        }

        // Test detail.
        String cleaned = bucketName.replaceFirst("^s3[an]?://", "");

        // Test detail.
        cleaned = cleaned.replaceFirst("/$", "");

        return cleaned;
    }
}
