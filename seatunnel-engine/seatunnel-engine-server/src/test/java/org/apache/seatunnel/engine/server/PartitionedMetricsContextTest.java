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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.JobMetricsPartitionUtils;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PartitionedMetricsContextTest extends AbstractSeaTunnelServerTest {

    @Test
    void testPartitionMetricsByImapKeyUsesMultipleKeys() {
        PipelineLocation pipelineLocation = new PipelineLocation(1001L, 1);
        List<TaskLocation> taskLocations =
                collectTaskLocationsAcrossPartitions(pipelineLocation, 3);
        HashMap<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
        for (int i = 0; i < taskLocations.size(); i++) {
            localMap.put(taskLocations.get(i), createMetricsContext("counter-" + i, i + 1));
        }

        Map<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> partitionedMetrics =
                TaskExecutionService.partitionMetricsByImapKey(localMap, getPartitionCount());

        Assertions.assertTrue(partitionedMetrics.size() > 1);
        Assertions.assertEquals(
                localMap.size(), partitionedMetrics.values().stream().mapToInt(Map::size).sum());
    }

    @Test
    void testGetOrCreateMetricsContextReadsOnlyMatchedPartition() {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        PipelineLocation pipelineLocation = new PipelineLocation(2001L, 1);
        List<TaskLocation> taskLocations =
                collectTaskLocationsAcrossPartitions(pipelineLocation, 2);
        TaskLocation targetTaskLocation = taskLocations.get(0);
        TaskLocation otherTaskLocation = taskLocations.get(1);

        HashMap<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
        localMap.put(targetTaskLocation, createMetricsContext("target-counter", 7));
        localMap.put(otherTaskLocation, createMetricsContext("other-counter", 3));
        TaskExecutionService.partitionMetricsByImapKey(localMap, getPartitionCount())
                .forEach(metricsIMap::put);

        TaskExecutionContext executionContext =
                new TaskExecutionContext(
                        null, (NodeEngineImpl) nodeEngine, server.getTaskExecutionService());
        SeaTunnelMetricsContext targetContext =
                executionContext.getOrCreateMetricsContext(targetTaskLocation);

        Assertions.assertEquals(7L, targetContext.counter("target-counter").getCount());
        Assertions.assertEquals(0L, targetContext.counter("other-counter").getCount());
    }

    @Test
    void testCoordinatorCleanupRemovesMetricsFromAllPartitions() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        PipelineLocation targetPipeline = new PipelineLocation(3001L, 1);
        PipelineLocation otherPipeline = new PipelineLocation(3002L, 1);

        putMetrics(targetPipeline, 2, "target");
        putMetrics(otherPipeline, 2, "other");

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(targetPipeline, PipelineStatus.FAILED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                targetPipeline,
                new PipelineCleanupRecord(
                        targetPipeline,
                        PipelineStatus.FAILED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(targetPipeline));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipeline));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(targetPipeline));
    }

    private void putMetrics(
            PipelineLocation pipelineLocation, int partitions, String metricPrefix) {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        HashMap<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
        List<TaskLocation> taskLocations =
                collectTaskLocationsAcrossPartitions(pipelineLocation, partitions);
        for (int i = 0; i < taskLocations.size(); i++) {
            localMap.put(taskLocations.get(i), createMetricsContext(metricPrefix + "-" + i, i + 1));
        }
        TaskExecutionService.partitionMetricsByImapKey(localMap, getPartitionCount())
                .forEach(
                        (partition, metricsByPartition) ->
                                metricsIMap.compute(
                                        partition,
                                        (key, centralMap) -> {
                                            if (centralMap == null) {
                                                centralMap = new HashMap<>();
                                            }
                                            centralMap.putAll(metricsByPartition);
                                            return centralMap;
                                        }));
    }

    private boolean hasMetricsForPipeline(PipelineLocation pipelineLocation) {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        return metricsIMap.values().stream()
                .filter(map -> map != null && !map.isEmpty())
                .flatMap(map -> map.keySet().stream())
                .anyMatch(
                        taskLocation ->
                                pipelineLocation.equals(
                                        taskLocation.getTaskGroupLocation().getPipelineLocation()));
    }

    private List<TaskLocation> collectTaskLocationsAcrossPartitions(
            PipelineLocation pipelineLocation, int partitionSize) {
        Map<Long, TaskLocation> taskLocationsByPartition = new LinkedHashMap<>();
        long taskGroupId = 1L;
        while (taskLocationsByPartition.size() < partitionSize && taskGroupId < 1024) {
            TaskLocation taskLocation =
                    new TaskLocation(
                            new TaskGroupLocation(
                                    pipelineLocation.getJobId(),
                                    pipelineLocation.getPipelineId(),
                                    taskGroupId),
                            taskGroupId,
                            0);
            long partition =
                    JobMetricsPartitionUtils.getMetricsImapPartition(
                            taskLocation, getPartitionCount());
            taskLocationsByPartition.putIfAbsent(partition, taskLocation);
            taskGroupId++;
        }
        Assertions.assertEquals(partitionSize, taskLocationsByPartition.size());
        return new ArrayList<>(taskLocationsByPartition.values());
    }

    private SeaTunnelMetricsContext createMetricsContext(String counterName, long count) {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        Counter counter = metricsContext.counter(counterName);
        counter.inc(count);
        return metricsContext;
    }

    private int getPartitionCount() {
        return server.getTaskExecutionService()
                .getSeaTunnelConfig()
                .getEngineConfig()
                .getJobMetricsPartitionCount();
    }
}
