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

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test helper. */
@Slf4j
public class S3BucketNameTest {

    @Test
    public void testBucketNameFormat() {
        log.info("test message");

        String originalBucket = S3TestConfig.S3_BUCKET;
        log.info("test message", originalBucket);

        // Test detail.
        String[] testCases = {
            "s3a://wt-auto-bucket",
            "wt-auto-bucket",
            "s3://wt-auto-bucket",
            "wt-auto-bucket/",
            "s3a://wt-auto-bucket/"
        };

        for (String testCase : testCases) {
            String cleanedBucket = cleanBucketName(testCase);
            log.info("test message", testCase, cleanedBucket);

            // Test detail.
            assertFalse(cleanedBucket.startsWith("s3://"), "test message");
            assertFalse(cleanedBucket.startsWith("s3a://"), "test message");
            assertFalse(cleanedBucket.endsWith("/"), "test message");
            assertTrue(cleanedBucket.length() > 0, "test message");
        }
    }

    @Test
    public void testCurrentConfiguration() {
        log.info("test message");

        String currentBucket = S3TestConfig.S3_BUCKET;
        String cleanedBucket = cleanBucketName(currentBucket);

        log.info("test message", currentBucket);
        log.info("test message", cleanedBucket);

        assertFalse(cleanedBucket.startsWith("s3://"));
        assertFalse(cleanedBucket.startsWith("s3a://"));
        assertFalse(cleanedBucket.endsWith("/"));
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
