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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class ClientJobExecutionEnvironmentTest {

    @Test
    public void testGetJobConfigParserShouldApplyVariables() throws Exception {
        Path configPath = Files.createTempFile("seatunnel-job", ".conf");
        Files.write(
                configPath,
                Collections.singletonList(
                        "env { job.name = \"client job\" }\n"
                                + "source { FakeSource { result_table_name = \"${my_table_name}\" } }\n"
                                + "sink { Console { source_table_name = \"fake\" } }"),
                StandardCharsets.UTF_8);
        try {
            SeaTunnelHazelcastClient hazelcastClient = Mockito.mock(SeaTunnelHazelcastClient.class);
            SeaTunnelConfig seaTunnelConfig = Mockito.mock(SeaTunnelConfig.class);
            TestingClientJobExecutionEnvironment jobEnvironment =
                    new TestingClientJobExecutionEnvironment(
                            new JobConfig(),
                            configPath.toString(),
                            Collections.singletonList("my_table_name=fake_table"),
                            hazelcastClient,
                            seaTunnelConfig,
                            1L);

            MultipleTableJobConfigParser parser = jobEnvironment.exposeJobConfigParser();
            Assertions.assertNotNull(parser);
            Config seaTunnelJobConfig = extractJobConfig(parser);
            Assertions.assertNotNull(seaTunnelJobConfig);

            List<? extends ConfigObject> sourceConfigs = seaTunnelJobConfig.getObjectList("source");
            Assertions.assertEquals(
                    "fake_table", sourceConfigs.get(0).toConfig().getString("result_table_name"));
        } finally {
            Files.deleteIfExists(configPath);
        }
    }

    private static Config extractJobConfig(MultipleTableJobConfigParser parser) throws Exception {
        Field seaTunnelJobConfigField =
                MultipleTableJobConfigParser.class.getDeclaredField("seaTunnelJobConfig");
        seaTunnelJobConfigField.setAccessible(true);
        return (Config) seaTunnelJobConfigField.get(parser);
    }

    private static final class TestingClientJobExecutionEnvironment
            extends ClientJobExecutionEnvironment {

        private TestingClientJobExecutionEnvironment(
                JobConfig jobConfig,
                String jobFilePath,
                List<String> variables,
                SeaTunnelHazelcastClient seaTunnelHazelcastClient,
                SeaTunnelConfig seaTunnelConfig,
                Long jobId) {
            super(
                    jobConfig,
                    jobFilePath,
                    variables,
                    seaTunnelHazelcastClient,
                    seaTunnelConfig,
                    jobId);
        }

        private MultipleTableJobConfigParser exposeJobConfigParser() {
            return getJobConfigParser();
        }
    }
}
