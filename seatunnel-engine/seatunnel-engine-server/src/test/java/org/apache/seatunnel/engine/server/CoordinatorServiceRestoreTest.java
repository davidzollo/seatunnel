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

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorServiceRestoreTest {

    @Test
    public void testRestoreJobUsesProvidedJobInfoTimestamp() throws Exception {
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceRestoreTest_testRestoreJobUsesProvidedJobInfoTimestamp"));
        CoordinatorService coordinatorService = null;
        try {
            SeaTunnelServer seaTunnelServer = mock(SeaTunnelServer.class);
            when(seaTunnelServer.isMasterNode()).thenReturn(false);
            coordinatorService =
                    new CoordinatorService(
                            instance.node.nodeEngine,
                            seaTunnelServer,
                            new EngineConfig(),
                            getClass().getClassLoader());

            Long jobId = 1L;
            IMap<Long, JobInfo> runningJobInfoIMap = mock(IMap.class);
            when(runningJobInfoIMap.get(jobId))
                    .thenThrow(new RetryableHazelcastException("IMap is loading"));
            IMap<Object, Object> runningJobStateIMap = mock(IMap.class);
            when(runningJobStateIMap.get(jobId)).thenReturn(JobStatus.RUNNING);

            setField(coordinatorService, "runningJobInfoIMap", runningJobInfoIMap);
            setField(coordinatorService, "runningJobStateIMap", runningJobStateIMap);
            setField(coordinatorService, "runningJobStateTimestampsIMap", mock(IMap.class));
            setField(coordinatorService, "ownedSlotProfilesIMap", mock(IMap.class));
            setField(coordinatorService, "metricsImap", mock(IMap.class));

            JobInfo jobInfo = new JobInfo(123L, createJobImmutableInformationData(instance, jobId));

            CoordinatorService restoreCoordinatorService = coordinatorService;
            PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
            try (MockedConstruction<JobMaster> mockedJobMaster =
                    mockConstruction(
                            JobMaster.class,
                            (mock, context) ->
                                    when(mock.getPhysicalPlan()).thenReturn(physicalPlan))) {
                Assertions.assertDoesNotThrow(
                        () ->
                                restoreJobFromMasterActiveSwitch(
                                        restoreCoordinatorService, jobId, jobInfo));
                JobMaster jobMaster = mockedJobMaster.constructed().get(0);
                verify(jobMaster).init(123L, true, getClass().getClassLoader());
                verify(runningJobInfoIMap, never()).get(jobId);
            }
        } finally {
            if (coordinatorService != null) {
                shutdownCoordinatorService(coordinatorService);
            }
            instance.shutdown();
        }
    }

    private static Data createJobImmutableInformationData(
            HazelcastInstanceImpl instance, Long jobId) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fake_to_console.conf",
                        "testRestoreJobUsesProvidedJobInfoTimestamp",
                        jobId);
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        instance.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return instance.getSerializationService().toData(jobImmutableInformation);
    }

    private static void restoreJobFromMasterActiveSwitch(
            CoordinatorService coordinatorService, Long jobId, JobInfo jobInfo) throws Exception {
        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);
        method.invoke(coordinatorService, jobId, jobInfo);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void shutdownCoordinatorService(CoordinatorService coordinatorService)
            throws Exception {
        shutdownExecutor(coordinatorService, "masterActiveListener");
        shutdownExecutor(coordinatorService, "pipelineCleanupScheduler");
        shutdownExecutor(coordinatorService, "executorService");
        Field runningJobMasterMapField =
                CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        runningJobMasterMapField.setAccessible(true);
        Map<Long, JobMaster> runningJobMasterMap =
                (Map<Long, JobMaster>) runningJobMasterMapField.get(coordinatorService);
        runningJobMasterMap.values().forEach(JobMaster::interrupt);
    }

    private static void shutdownExecutor(CoordinatorService coordinatorService, String fieldName)
            throws Exception {
        Field field = CoordinatorService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        Object executor = field.get(coordinatorService);
        if (executor instanceof ScheduledExecutorService) {
            ((ScheduledExecutorService) executor).shutdownNow();
        } else if (executor instanceof ExecutorService) {
            ((ExecutorService) executor).shutdownNow();
        }
    }
}
