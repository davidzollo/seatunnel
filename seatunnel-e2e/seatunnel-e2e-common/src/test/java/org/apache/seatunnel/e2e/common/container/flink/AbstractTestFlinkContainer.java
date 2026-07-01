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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test. The before method will create a Flink
 * cluster, and after method will close the Flink cluster. You can use {@link
 * TestContainer#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@Slf4j
public abstract class AbstractTestFlinkContainer extends AbstractTestContainer {

    protected static final List<String> DEFAULT_FLINK_PROPERTIES =
            Arrays.asList(
                    "jobmanager.rpc.address: jobmanager",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 4",
                    "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false",
                    // limit restart attempts in e2e to avoid infinite retries
                    "restart-strategy: fixed-delay",
                    "restart-strategy.fixed-delay.attempts: 2",
                    "restart-strategy.fixed-delay.delay: 1000");

    protected static final String DEFAULT_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";

    /**
     * Cache the expected Flink job name for tests that submit a logical SeaTunnel job id.
     *
     * <p>Flink exposes its own runtime JID in the REST API, so E2E tests need a stable way to
     * resolve the logical id they passed to {@code executeJob(..., jobId, ...)} back to the live
     * Flink job entry.
     */
    private final Map<String, String> trackedJobNames = new ConcurrentHashMap<>();

    /** Cache the resolved runtime Flink JID once a logical job id has been matched. */
    private final Map<String, String> trackedFlinkJobIds = new ConcurrentHashMap<>();

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        FileUtils.createNewDir(HOST_VOLUME_MOUNT_PATH);
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());
        jobManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts()
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(dockerImage + ":jobmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Starting the resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE);
        copySeaTunnelStarterToContainer(jobManager);
        copySeaTunnelStarterLoggingToContainer(jobManager);
        jobManager.setPortBindings(Lists.newArrayList(String.format("%s:%s", 8081, 8081)));

        taskManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                dockerImage + ":taskmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(
                                                ".*Successful registration at resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE);

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        executeExtraCommands(jobManager);
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    @Override
    public void tearDown() throws Exception {
        if (taskManager != null) {
            // delete the volume
            taskManager.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            taskManager.stop();
        }
        if (jobManager != null) {
            // delete the volume
            jobManager.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            jobManager.stop();
        }
        FileUtils.deleteFile(HOST_VOLUME_MOUNT_PATH);
    }

    @Override
    protected String getSavePointCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getCancelJobCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getRestoreCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }

    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(jobManager);
        extendedFactory.extend(taskManager);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, Collections.emptyList());
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(jobManager, confFile, null, variables);
    }

    @Override
    public Container.ExecResult executeJob(String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        trackLogicalJob(jobId, confFile);
        log.info("test in container: {}", identifier());
        return executeJob(
                jobManager,
                confFile,
                jobId,
                variables != null ? Arrays.asList(variables) : Collections.emptyList());
    }

    @Override
    public String getServerLogs() {
        return jobManager.getLogs() + "\n" + taskManager.getLogs();
    }

    public String executeJobManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return jobManager.execInContainer("bash", "-c", command).getStdout();
    }

    @Override
    public String getJobStatus(String jobId) {
        try {
            Map<String, Object> matchedJob = findTrackedJob(jobId);
            if (matchedJob == null) {
                return null;
            }
            Object state = matchedJob.get("state");
            return state == null ? null : state.toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while querying Flink job status for " + jobId, e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to query Flink job status for " + jobId, e);
        }
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, jobManager);
    }

    @Override
    public void copyAbsolutePathToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(Paths.get(path), targetPath, jobManager);
    }

    /** Remember the config-derived Flink job name for later REST lookups by logical job id. */
    protected final void trackLogicalJob(String jobId, String confFile) {
        if (jobId == null || jobId.isEmpty()) {
            return;
        }
        trackedJobNames.put(jobId, new File(confFile).getName());
    }

    /**
     * Resolve the live Flink job entry for either a runtime JID or a logical SeaTunnel test job id.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> findTrackedJob(String jobId)
            throws IOException, InterruptedException {
        Map<String, Object> jobInfo =
                JsonUtils.toMap(
                        executeJobManagerInnerCommand(
                                "curl -s http://localhost:8081/jobs/overview"),
                        String.class,
                        Object.class);
        Object jobsValue = jobInfo.get("jobs");
        if (!(jobsValue instanceof List)) {
            return null;
        }
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) jobsValue;

        String cachedFlinkJobId = trackedFlinkJobIds.get(jobId);
        if (cachedFlinkJobId != null) {
            for (Map<String, Object> job : jobs) {
                if (cachedFlinkJobId.equals(String.valueOf(job.get("jid")))) {
                    return job;
                }
            }
            trackedFlinkJobIds.remove(jobId);
        }

        for (Map<String, Object> job : jobs) {
            if (jobId.equals(String.valueOf(job.get("jid")))) {
                return job;
            }
        }

        String trackedJobName = trackedJobNames.get(jobId);
        if (trackedJobName == null) {
            return null;
        }

        for (Map<String, Object> job : jobs) {
            if (trackedJobName.equals(String.valueOf(job.get("name")))) {
                Object runtimeJobId = job.get("jid");
                if (runtimeJobId != null) {
                    trackedFlinkJobIds.put(jobId, runtimeJobId.toString());
                }
                return job;
            }
        }
        return null;
    }
}
