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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class FakeSourceSplitEnumeratorTest {

    @Test
    void shouldSignalNoMoreSplitsWhenRestoreAlreadyConsumesAllSplits() throws Exception {
        ReadonlyConfig testConfig = getTestConfigFile("simple.schema.conf");
        MultipleTableFakeSourceConfig sourceConfig = new MultipleTableFakeSourceConfig(testConfig);
        FakeConfig fakeConfig = sourceConfig.getFakeConfigs().get(0);
        String tableId = fakeConfig.getCatalogTable().getTableId().toTablePath().toString();

        // Simulate the recovery state where checkpoint restore already recorded the only split as
        // assigned, so discovery creates no pending split for the registered reader.
        Set<FakeSourceSplit> restoredAssignedSplits =
                Collections.singleton(
                        new FakeSourceSplit(tableId, 0, fakeConfig.getRowNum(), 0, 1));
        TestSplitEnumeratorContext context =
                new TestSplitEnumeratorContext(1, Collections.singleton(0));
        FakeSourceSplitEnumerator enumerator =
                new FakeSourceSplitEnumerator(context, sourceConfig, restoredAssignedSplits);

        enumerator.run();

        Assertions.assertTrue(
                context.assignedSplitsByReader.isEmpty(),
                "No new split should be assigned after restore already consumed all splits.");
        Assertions.assertEquals(
                Collections.singletonList(0),
                context.noMoreSplitsReaders,
                "The registered reader must still receive the terminal no-more-splits signal.");
        Assertions.assertEquals(
                0,
                enumerator.currentUnassignedSplitSize(),
                "Restore path should not leave any pending split behind.");
    }

    private ReadonlyConfig getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        if (!configFile.startsWith("/")) {
            configFile = "/" + configFile;
        }
        URL resource = FakeSourceSplitEnumeratorTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        String path = Paths.get(resource.toURI()).toString();
        Config config = ConfigFactory.parseFile(new File(path));
        Assertions.assertTrue(config.hasPath("FakeSource"));
        return ReadonlyConfig.fromConfig(config.getConfig("FakeSource"));
    }

    private static final class TestSplitEnumeratorContext
            implements SourceSplitEnumerator.Context<FakeSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<FakeSourceSplit>> assignedSplitsByReader =
                new LinkedHashMap<>();
        private final List<Integer> noMoreSplitsReaders = new ArrayList<>();

        private TestSplitEnumeratorContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = new LinkedHashSet<>(registeredReaders);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<FakeSourceSplit> splits) {
            assignedSplitsByReader.put(subtaskId, new ArrayList<>(splits));
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsReaders.add(subtask);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return new AbstractMetricsContext() {};
        }

        @Override
        public EventListener getEventListener() {
            return new EventListener() {
                @Override
                public void onEvent(Event event) {}
            };
        }
    }
}
