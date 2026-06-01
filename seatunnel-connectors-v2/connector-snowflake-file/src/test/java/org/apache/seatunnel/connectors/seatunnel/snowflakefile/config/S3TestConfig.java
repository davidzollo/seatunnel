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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

public class S3TestConfig {

    // Test detail.
    public static final String S3_BUCKET =
            getEnvOrDefault("SNOWFLAKE_FILE_TEST_S3_BUCKET", "s3a://example-bucket");
    public static final String S3_REGION =
            getEnvOrDefault("SNOWFLAKE_FILE_TEST_S3_REGION", "us-east-1");
    public static final String S3_ENDPOINT =
            getEnvOrDefault("SNOWFLAKE_FILE_TEST_S3_ENDPOINT", "s3.amazonaws.com");
    public static final String ACCESS_KEY =
            getEnvOrDefault("SNOWFLAKE_FILE_TEST_AWS_ACCESS_KEY_ID", "test-access-key");
    public static final String SECRET_KEY =
            getEnvOrDefault("SNOWFLAKE_FILE_TEST_AWS_SECRET_ACCESS_KEY", "test-secret-key");
    public static final String CREDENTIALS_PROVIDER =
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

    // Test detail.
    public static final String HADOOP_S3_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    public static final String S3A_IMPL_DISABLE_CACHE = "true";

    // Test detail.
    public static final String SNOWFLAKE_ACCOUNT = "test_account";
    public static final String SNOWFLAKE_WAREHOUSE = "test_warehouse";
    public static final String SNOWFLAKE_DATABASE = "test_database";
    public static final String SNOWFLAKE_SCHEMA = "test_schema";
    public static final String SNOWFLAKE_TABLE = "test_table";
    public static final String SNOWFLAKE_USER = "test_user";
    public static final String SNOWFLAKE_PASSWORD = "test_password";

    /** Test helper. */
    public static Config createS3TestConfig() {
        Map<String, Object> configMap = new HashMap<>();

        // Test detail.
        configMap.put("account", SNOWFLAKE_ACCOUNT);
        configMap.put("warehouse", SNOWFLAKE_WAREHOUSE);
        configMap.put("database", SNOWFLAKE_DATABASE);
        configMap.put("schema", SNOWFLAKE_SCHEMA);
        configMap.put("table", SNOWFLAKE_TABLE);
        configMap.put("user", SNOWFLAKE_USER);
        configMap.put("password", SNOWFLAKE_PASSWORD);

        // Test detail.
        configMap.put("s3_bucket", S3_BUCKET);
        configMap.put("s3_protocol", "s3china"); // Test detail.
        configMap.put("s3_region", S3_REGION);
        configMap.put("s3_key_prefix", "seatunnel-test/");
        configMap.put("aws_access_key_id", ACCESS_KEY);
        configMap.put("aws_secret_access_key", SECRET_KEY);

        // Test detail.
        configMap.put("file_format", "CSV");
        configMap.put("field_delimiter", ",");
        configMap.put("record_delimiter", "\\n");
        configMap.put("file_extension", ".csv");

        // Test detail.
        configMap.put("buffer_size", 1048576); // 1MB
        configMap.put("max_file_size", 10485760); // Test detail.
        configMap.put("purge_after_copy", false); // Test detail.

        // Test detail.
        configMap.put("time_format", "HH24:MI:SS");
        configMap.put("date_format", "YYYY-MM-DD");
        configMap.put("timestamp_format", "YYYY-MM-DD HH24:MI:SS.FF3");

        // Test detail.
        configMap.put("fs.s3a.endpoint", S3_ENDPOINT);
        configMap.put("fs.s3a.aws.credentials.provider", CREDENTIALS_PROVIDER);
        configMap.put("fs.s3a.impl.disable.cache", S3A_IMPL_DISABLE_CACHE);

        return ConfigFactory.parseMap(configMap);
    }

    /** Test helper. */
    public static Config createS3ConnectionTestConfig() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("s3_bucket", S3_BUCKET);
        configMap.put("s3_protocol", "s3china"); // Test detail.
        configMap.put("s3_region", S3_REGION);
        configMap.put("aws_access_key_id", ACCESS_KEY);
        configMap.put("aws_secret_access_key", SECRET_KEY);
        configMap.put("fs.s3a.endpoint", S3_ENDPOINT);
        configMap.put("fs.s3a.aws.credentials.provider", CREDENTIALS_PROVIDER);
        configMap.put("fs.s3a.impl.disable.cache", S3A_IMPL_DISABLE_CACHE);

        return ConfigFactory.parseMap(configMap);
    }

    /** Test helper. */
    public static String getS3AUrl(String keyPrefix) {
        return String.format("s3a://%s/%s", S3_BUCKET.replace("s3a://", ""), keyPrefix);
    }

    /** Test helper. */
    public static boolean isS3ConfigValid() {
        return System.getenv("SNOWFLAKE_FILE_TEST_S3_BUCKET") != null
                && System.getenv("SNOWFLAKE_FILE_TEST_AWS_ACCESS_KEY_ID") != null
                && System.getenv("SNOWFLAKE_FILE_TEST_AWS_SECRET_ACCESS_KEY") != null;
    }

    private static String getEnvOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }
}
