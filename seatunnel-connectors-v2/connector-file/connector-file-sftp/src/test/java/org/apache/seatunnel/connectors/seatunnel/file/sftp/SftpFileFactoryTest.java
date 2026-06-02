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

package org.apache.seatunnel.connectors.seatunnel.file.sftp;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.config.SftpConf;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.config.SftpConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.sink.SftpFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.source.SftpFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.system.SFTPFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies SFTP factory rules and configuration translation used before creating source and sink
 * file systems.
 */
class SftpFileFactoryTest {

    /** Verifies that both source and sink factories accept the new filename encoding option. */
    @Test
    void optionRule() {
        OptionRule sourceOptionRule = (new SftpFileSourceFactory()).optionRule();
        OptionRule sinkOptionRule = (new SftpFileSinkFactory()).optionRule();
        Assertions.assertNotNull(sourceOptionRule);
        Assertions.assertNotNull(sinkOptionRule);
        Assertions.assertTrue(
                sourceOptionRule
                        .getOptionalOptions()
                        .contains(SftpConfigOptions.SFTP_FILENAME_ENCODING));
        Assertions.assertTrue(
                sinkOptionRule
                        .getOptionalOptions()
                        .contains(SftpConfigOptions.SFTP_FILENAME_ENCODING));
    }

    /** Verifies that jobs without explicit filename encoding keep the JSch default behavior. */
    @Test
    void shouldNotSetFilenameEncodingWhenOptionIsMissing() {
        HadoopConf hadoopConf =
                SftpConf.buildWithConfig(ReadonlyConfig.fromMap(createBaseConfig()));

        Assertions.assertFalse(
                hadoopConf.getExtraOptions().containsKey(SFTPFileSystem.FS_SFTP_FILENAME_ENCODING));
    }

    /**
     * Verifies that content encoding keeps its old meaning and does not rewrite file name decoding.
     */
    @Test
    void shouldNotReuseContentEncodingAsFilenameEncoding() {
        Map<String, Object> config = createBaseConfig();
        config.put(SftpConfigOptions.ENCODING.key(), "GB18030");

        HadoopConf hadoopConf = SftpConf.buildWithConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertFalse(
                hadoopConf.getExtraOptions().containsKey(SFTPFileSystem.FS_SFTP_FILENAME_ENCODING));
    }

    /** Verifies that an explicit filename encoding overrides the content encoding fallback. */
    @Test
    void shouldPassConfiguredFilenameEncodingToHadoopConf() {
        Map<String, Object> config = createBaseConfig();
        config.put(SftpConfigOptions.ENCODING.key(), "UTF-8");
        config.put(SftpConfigOptions.SFTP_FILENAME_ENCODING.key(), "GB18030");

        HadoopConf hadoopConf = SftpConf.buildWithConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(
                "GB18030",
                hadoopConf.getExtraOptions().get(SFTPFileSystem.FS_SFTP_FILENAME_ENCODING));
    }

    /** Builds the minimal SFTP config shared by the factory tests. */
    private Map<String, Object> createBaseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SftpConfigOptions.SFTP_HOST.key(), "127.0.0.1");
        config.put(SftpConfigOptions.SFTP_PORT.key(), 22);
        config.put(SftpConfigOptions.SFTP_USER.key(), "user");
        config.put(SftpConfigOptions.SFTP_PASSWORD.key(), "password");
        return config;
    }
}
