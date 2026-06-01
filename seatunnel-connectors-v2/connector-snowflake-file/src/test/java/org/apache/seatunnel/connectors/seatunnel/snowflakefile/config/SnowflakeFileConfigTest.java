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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnowflakeFileConfigTest {

    @Test
    public void testParseLocalFileConfig() {
        Config config =
                ConfigFactory.parseString(
                        "account = \"test_account\"\n"
                                + "warehouse = \"test_wh\"\n"
                                + "database = \"test_db\"\n"
                                + "schema = \"public\"\n"
                                + "table = \"orders\"\n"
                                + "user = \"test_user\"\n"
                                + "password = \"test_password\"\n"
                                + "staging_backend = \"LOCAL_FILE\"\n"
                                + "local_stage_type = \"USER\"\n"
                                + "local_stage_prefix = \"seatunnel-local\"\n"
                                + "file_format = \"CSV\"\n"
                                + "field_delimiter = \",\"\n"
                                + "record_delimiter = \"\\n\"\n"
                                + "file_extension = \".csv\"\n"
                                + "buffer_size = 1024\n"
                                + "max_file_size = 2048\n"
                                + "purge_after_copy = true\n"
                                + "time_format = \"HH24:MI:SS\"\n"
                                + "date_format = \"YYYY-MM-DD\"\n"
                                + "timestamp_format = \"YYYY-MM-DD HH24:MI:SS.FF3\"\n");

        SnowflakeFileConfig snowflakeFileConfig = new SnowflakeFileConfig(config);

        assertEquals(
                SnowflakeFileConfig.StagingBackend.LOCAL_FILE,
                snowflakeFileConfig.getStagingBackend());
        assertEquals(
                SnowflakeFileConfig.LocalStageType.USER, snowflakeFileConfig.getLocalStageType());
        assertEquals("seatunnel-local", snowflakeFileConfig.getLocalStagePrefix());
        assertNull(snowflakeFileConfig.getS3Bucket());
        assertNotNull(snowflakeFileConfig.getLocalTempDir());
        assertTrue(snowflakeFileConfig.getLocalTempDir().contains("snowflake"));
    }

    @Test
    public void testParseLocalFileConfigWithoutWarehouse() {
        Config config =
                ConfigFactory.parseString(
                        "account = \"test_account\"\n"
                                + "database = \"test_db\"\n"
                                + "schema = \"public\"\n"
                                + "table = \"orders\"\n"
                                + "user = \"test_user\"\n"
                                + "password = \"test_password\"\n"
                                + "staging_backend = \"LOCAL_FILE\"\n"
                                + "file_format = \"CSV\"\n"
                                + "field_delimiter = \",\"\n"
                                + "record_delimiter = \"\\n\"\n"
                                + "file_extension = \".csv\"\n"
                                + "buffer_size = 1024\n"
                                + "max_file_size = 2048\n"
                                + "purge_after_copy = true\n"
                                + "time_format = \"HH24:MI:SS\"\n"
                                + "date_format = \"YYYY-MM-DD\"\n"
                                + "timestamp_format = \"YYYY-MM-DD HH24:MI:SS.FF3\"\n");

        SnowflakeFileConfig snowflakeFileConfig = new SnowflakeFileConfig(config);

        assertNull(snowflakeFileConfig.getWarehouse());
        assertEquals(
                SnowflakeFileConfig.StagingBackend.LOCAL_FILE,
                snowflakeFileConfig.getStagingBackend());
    }
}
