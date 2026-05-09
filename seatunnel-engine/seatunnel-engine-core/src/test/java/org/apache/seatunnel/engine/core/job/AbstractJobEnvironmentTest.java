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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.Config;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AbstractJobEnvironmentTest {

    @Test
    public void testAddCommonPluginJarsFromEnvOptions() throws Exception {
        Path jarPath = Files.createTempFile("seatunnel-custom-udf", ".jar");
        try {
            TestingJobEnvironment jobEnvironment = new TestingJobEnvironment();
            ReadonlyConfig envOptions =
                    ReadonlyConfig.fromMap(
                            Collections.singletonMap("jars", jarPath.toUri().toString()));

            jobEnvironment.addEnvOptions(envOptions);
            jobEnvironment.addEnvOptions(envOptions);

            List<URL> commonPluginJars = jobEnvironment.getCommonPluginJars();
            Assertions.assertEquals(1, commonPluginJars.size());
            Assertions.assertEquals(jarPath.toUri().toURL(), commonPluginJars.get(0));
        } finally {
            Files.deleteIfExists(jarPath);
        }
    }

    @Test
    public void testLogicalDagGeneratorShouldKeepSavePointFlag() {
        TestingJobEnvironment jobEnvironment = new TestingJobEnvironment(true);
        jobEnvironment.addAction(new TestingAction(1L));

        LogicalDag logicalDag = jobEnvironment.generateLogicalDag();

        Assertions.assertTrue(logicalDag.isStartWithSavePoint());
    }

    private static final class TestingJobEnvironment extends AbstractJobEnvironment {

        private TestingJobEnvironment() {
            this(false);
        }

        private TestingJobEnvironment(boolean isStartWithSavePoint) {
            super(new JobConfig(), isStartWithSavePoint);
        }

        private void addEnvOptions(ReadonlyConfig envOptions) {
            addCommonPluginJarsFromEnvOptions(envOptions);
        }

        private void addAction(Action action) {
            this.actions.add(action);
        }

        private LogicalDag generateLogicalDag() {
            return getLogicalDagGenerator().generate();
        }

        private List<URL> getCommonPluginJars() {
            return commonPluginJars;
        }

        @Override
        protected Set<URL> searchPluginJars() {
            return Collections.emptySet();
        }

        @Override
        protected MultipleTableJobConfigParser getJobConfigParser() {
            return null;
        }

        @Override
        protected LogicalDag getLogicalDag() {
            return null;
        }
    }

    private static final class TestingAction implements Action {

        private final long id;
        private String name;
        private int parallelism;
        private final List<Action> upstream;
        private final Set<URL> jarUrls;
        private final Set<ConnectorJarIdentifier> connectorJarIdentifiers;

        private TestingAction(long id) {
            this.id = id;
            this.name = "testing-action";
            this.parallelism = 1;
            this.upstream = new ArrayList<>();
            this.jarUrls = new HashSet<>();
            this.connectorJarIdentifiers = new HashSet<>();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void setName(String name) {
            this.name = name;
        }

        @Override
        public List<Action> getUpstream() {
            return upstream;
        }

        @Override
        public void addUpstream(Action action) {
            upstream.add(action);
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public Set<URL> getJarUrls() {
            return jarUrls;
        }

        @Override
        public Set<ConnectorJarIdentifier> getConnectorJarIdentifiers() {
            return connectorJarIdentifiers;
        }

        @Override
        public Config getConfig() {
            return null;
        }
    }
}
