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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineRetryableException;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.service.jar.ConnectorPackageService;
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.telemetry.log.TaskLogManagerService;

import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SeaTunnelServer
        implements ManagedService, MembershipAwareService, LiveOperationsTracker {

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);

    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private final LiveOperationRegistry liveOperationRegistry;

    private volatile SlotService slotService;
    private TaskExecutionService taskExecutionService;
    private ClassLoaderService classLoaderService;
    private CoordinatorService coordinatorService;
    @Getter private CheckpointService checkpointService;
    private ScheduledExecutorService monitorService;
    private TaskLogManagerService taskLogManagerService;

    @Getter private SeaTunnelHealthMonitor seaTunnelHealthMonitor;

    private final SeaTunnelConfig seaTunnelConfig;

    private volatile boolean isRunning = true;

    public SeaTunnelServer(@NonNull SeaTunnelConfig seaTunnelConfig) {
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        LOGGER.info("SeaTunnel server start...");
    }

    /** Lazy load for Slot Service */
    public SlotService getSlotService() {
        if (slotService == null) {
            synchronized (this) {
                if (slotService == null) {
                    SlotService service =
                            new DefaultSlotService(
                                    nodeEngine,
                                    taskExecutionService,
                                    seaTunnelConfig.getEngineConfig().getSlotServiceConfig());
                    service.init();
                    slotService = service;
                }
            }
        }
        return slotService;
    }

    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        // TODO Determine whether to execute there method on the master node according to the deploy
        // type
        classLoaderService =
                new DefaultClassLoaderService(
                        seaTunnelConfig.getEngineConfig().isClassloaderCacheMode());
        taskExecutionService =
                new TaskExecutionService(
                        classLoaderService, nodeEngine, nodeEngine.getProperties());
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(taskExecutionService);
        taskExecutionService.start();
        getSlotService();

        List<URL> jars;
        try {
            jars = FileUtils.searchJarFiles(Common.appStarterDir().resolve("zeta"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader = new URLClassLoader(jars.toArray(new URL[0]));
        LOGGER.info("init seatunnel server with jars: " + jars);

        Thread.currentThread().setContextClassLoader(classLoader);
        coordinatorService =
                new CoordinatorService(
                        nodeEngine, this, seaTunnelConfig.getEngineConfig(), classLoader);
        checkpointService =
                new CheckpointService(seaTunnelConfig.getEngineConfig().getCheckpointConfig());
        monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(
                this::printExecutionInfo,
                0,
                seaTunnelConfig.getEngineConfig().getPrintExecutionInfoInterval(),
                TimeUnit.SECONDS);

        // task log manager service
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs().isEnabled()) {
            taskLogManagerService =
                    new TaskLogManagerService(
                            seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs());
            taskLogManagerService.initClean();
        }

        seaTunnelHealthMonitor = new SeaTunnelHealthMonitor(((NodeEngineImpl) engine).getNode());
        Thread.currentThread().setContextClassLoader(appClassLoader);
    }

    @Override
    public void reset() {}

    @Override
    public void shutdown(boolean terminate) {
        isRunning = false;
        if (taskExecutionService != null) {
            taskExecutionService.shutdown();
        }
        if (classLoaderService != null) {
            classLoaderService.close();
        }
        if (monitorService != null) {
            monitorService.shutdownNow();
        }
        if (slotService != null) {
            slotService.close();
        }
        if (coordinatorService != null) {
            coordinatorService.shutdown();
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {}

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        try {
            if (isMasterNode()) {
                this.getCoordinatorService().memberRemoved(event);
            }
        } catch (SeaTunnelEngineException e) {
            LOGGER.severe("Error when handle member removed event", e);
        }
    }

    @Override
    public void populate(LiveOperations liveOperations) {}

    /** Used for debugging on call */
    public String printMessage(String message) {
        LOGGER.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    public CoordinatorService getCoordinatorService() {
        int retryCount = 0;
        if (isMasterNode()) {
            int maxRetry = 3;
            int retryPause = 500;
            while (isRunning
                    && retryCount < maxRetry
                    && !coordinatorService.isCoordinatorActive()
                    && isMasterNode()) {
                try {
                    LOGGER.warning(
                            "This is master node, waiting the coordinator service init finished");
                    Thread.sleep(retryPause);
                    retryCount++;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (coordinatorService.isCoordinatorActive()) {
                return coordinatorService;
            }

            if (!isMasterNode()) {
                throw new SeaTunnelEngineException("This is not a master node now.");
            }
            // Return retryable exception to retry from the worker node, because the coordinator is
            // not ready yet. By this way, we can release the operation thread and retry later.
            throw new SeaTunnelEngineRetryableException(
                    "Can not get coordinator service from an active master node.");
        } else {
            throw new SeaTunnelEngineException(
                    "Please don't get coordinator service from an inactive master node");
        }
    }

    public TaskExecutionService getTaskExecutionService() {
        return taskExecutionService;
    }

    public ClassLoaderService getClassLoaderService() {
        return classLoaderService;
    }

    /**
     * return whether task is end
     *
     * @param taskGroupLocation taskGroupLocation
     */
    public boolean taskIsEnded(@NonNull TaskGroupLocation taskGroupLocation) {
        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);

        Object taskState = runningJobState.get(taskGroupLocation);
        return taskState != null && ((ExecutionState) taskState).isEndState();
    }

    public boolean isMasterNode() {
        // must retry until the cluster have master node
        try {
            return RetryUtils.retryWithException(
                    () -> nodeEngine.getThisAddress().equals(nodeEngine.getMasterAddress()),
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> exception instanceof NullPointerException && isRunning,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (InterruptedException e) {
            LOGGER.info("master node check interrupted");
            return false;
        } catch (Exception e) {
            throw new SeaTunnelEngineException("cluster have no master node", e);
        }
    }

    private void printExecutionInfo() {
        coordinatorService.printExecutionInfo();
        if (coordinatorService.isCoordinatorActive() && this.isMasterNode()) {
            coordinatorService.printJobDetailInfo();
        }
    }

    public SeaTunnelConfig getSeaTunnelConfig() {
        return seaTunnelConfig;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public ConnectorPackageService getConnectorPackageService() {
        return getCoordinatorService().getConnectorPackageService();
    }

    public TaskLogManagerService getTaskLogManagerService() {
        return taskLogManagerService;
    }
}
