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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

class FailedPipelineMetricsCleanupTest extends AbstractSeaTunnelServerTest {

    @Test
    void testEnqueuePipelineCleanupIfNeededAcceptsFailedStatus() {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId + 1000, 1);
        JobMaster jobMaster = null;
        try {
            jobMaster = newJobInstanceWithRunningState(jobId);
            jobMaster.enqueuePipelineCleanupIfNeeded(pipelineLocation, PipelineStatus.FAILED);

            IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                    nodeEngine
                            .getHazelcastInstance()
                            .getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
            PipelineCleanupRecord record = pendingCleanupIMap.get(pipelineLocation);
            Assertions.assertNotNull(record);
            Assertions.assertEquals(PipelineStatus.FAILED, record.getFinalStatus());
            Assertions.assertFalse(record.isSavepointEnd());
        } finally {
            removePendingCleanupRecord(pipelineLocation);
            cancelJob(jobMaster);
        }
    }

    @Test
    void testRemoveMetricsContextAcceptsFailedStatus() {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId + 2000, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 2001, 1);
        JobMaster jobMaster = null;
        try {
            jobMaster = newJobInstanceWithRunningState(jobId);
            putMetrics(pipelineLocation, otherPipelineLocation);

            jobMaster.removeMetricsContext(pipelineLocation, PipelineStatus.FAILED);

            Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
            Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));
        } finally {
            clearMetrics();
            cancelJob(jobMaster);
        }
    }

    @Test
    void testCoordinatorCleanupRemovesFailedPipelineMetricsAndRecord() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 1, 1);
        putMetrics(pipelineLocation, otherPipelineLocation);

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FAILED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FAILED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));

        runningJobStateIMap.remove(pipelineLocation);
        clearMetrics();
    }

    private JobMaster newJobInstanceWithRunningState(long jobId) {
        startJob(jobId, "stream_fakesource_to_file.conf", false);
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));
        return jobMaster;
    }

    private void cancelJob(JobMaster jobMaster) {
        if (jobMaster == null) {
            return;
        }
        try {
            jobMaster.cancelJob();
        } catch (Exception ignored) {
            // Ignore cleanup failures in test teardown.
        }
    }

    private void putMetrics(PipelineLocation... pipelineLocations) {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap = new HashMap<>();
        for (PipelineLocation pipelineLocation : pipelineLocations) {
            TaskGroupLocation taskGroupLocation =
                    new TaskGroupLocation(
                            pipelineLocation.getJobId(), pipelineLocation.getPipelineId(), 1L);
            TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);
            centralMap.put(taskLocation, new SeaTunnelMetricsContext());
        }
        metricsIMap.put(Constant.IMAP_RUNNING_JOB_METRICS_KEY, centralMap);
    }

    private void clearMetrics() {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        metricsIMap.remove(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
    }

    private void removePendingCleanupRecord(PipelineLocation pipelineLocation) {
        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.remove(pipelineLocation);
    }

    private boolean hasMetricsForPipeline(PipelineLocation pipelineLocation) {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        Map<TaskLocation, SeaTunnelMetricsContext> centralMap =
                metricsIMap.get(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
        return centralMap != null
                && centralMap.keySet().stream()
                        .anyMatch(
                                taskLocation ->
                                        pipelineLocation.equals(
                                                taskLocation
                                                        .getTaskGroupLocation()
                                                        .getPipelineLocation()));
    }

    private void awaitCoordinatorActive(CoordinatorService coordinatorService) {
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
    }
}
