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

package org.apache.seatunnel.e2e.common.container.flink;

import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Verifies that Flink E2E containers can resolve a logical SeaTunnel job id back to the live Flink
 * runtime job entry exposed by the REST API.
 */
public class AbstractTestFlinkContainerTest {

    @Test
    public void shouldResolveRuntimeFlinkJobIdDirectly() {
        MockFlinkContainer container = new MockFlinkContainer();
        container.addJobOverview(
                "{\"jobs\":[{\"jid\":\"actual-flink-job\",\"name\":\"job.conf\",\"state\":\"RUNNING\"}]}");

        Assertions.assertEquals("RUNNING", container.getJobStatus("actual-flink-job"));
    }

    @Test
    public void shouldResolveLogicalSeaTunnelJobIdByTrackedConfigName() {
        MockFlinkContainer container = new MockFlinkContainer();
        container.trackLogicalJob(
                "logical-seatunnel-job", "/kafka/kafka_to_kafka_exactly_once_streaming.conf");
        container.addJobOverview(
                "{\"jobs\":[{\"jid\":\"actual-flink-job\",\"name\":\"kafka_to_kafka_exactly_once_streaming.conf\",\"state\":\"RUNNING\"}]}");

        Assertions.assertEquals("RUNNING", container.getJobStatus("logical-seatunnel-job"));
    }

    /** Lightweight test double that feeds canned Flink REST responses into the shared resolver. */
    private static final class MockFlinkContainer extends AbstractTestFlinkContainer {

        private final Deque<String> jobOverviewResponses = new ArrayDeque<>();

        private void addJobOverview(String response) {
            jobOverviewResponses.addLast(response);
        }

        @Override
        public TestContainerId identifier() {
            return TestContainerId.FLINK_1_20;
        }

        @Override
        protected String getStartModuleName() {
            return "seatunnel-flink-starter" + File.separator + "seatunnel-flink-20-starter";
        }

        @Override
        protected String getStartShellName() {
            return "start-seatunnel-flink-20-connector-v2.sh";
        }

        @Override
        protected String getConnectorModulePath() {
            return "seatunnel-connectors-v2";
        }

        @Override
        protected String getConnectorType() {
            return "seatunnel";
        }

        @Override
        protected String getConnectorNamePrefix() {
            return "connector-";
        }

        @Override
        protected String getSavePointCommand() {
            return "-s";
        }

        @Override
        protected String getCancelJobCommand() {
            return "-can";
        }

        @Override
        protected String getRestoreCommand() {
            return "-r";
        }

        @Override
        public String executeJobManagerInnerCommand(String command) {
            Assertions.assertEquals("curl -s http://localhost:8081/jobs/overview", command);
            return jobOverviewResponses.removeFirst();
        }
    }
}
