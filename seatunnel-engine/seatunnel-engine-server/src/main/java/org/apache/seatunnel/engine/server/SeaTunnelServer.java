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
import org.apache.seatunnel.engine.common.config.EngineConfig;
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
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
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
    private JettyService jettyService;
    private TaskLogManagerService taskLogManagerService;

    @Getter private SeaTunnelHealthMonitor seaTunnelHealthMonitor;

    private final SeaTunnelConfig seaTunnelConfig;

    private volatile boolean isRunning = true;

    @Getter private EventService eventService;

    public SeaTunnelServer(@NonNull SeaTunnelConfig seaTunnelConfig) {
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        LOGGER.info("SeaTunnel server start...");
    }

    /** Lazy load for Slot Service */
    public SlotService getSlotService() {
        // If the node is master node, the slot service is not needed.
        if (EngineConfig.ClusterRole.MASTER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            return null;
        }

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
        try {
            initInternal((NodeEngineImpl) engine);
        } catch (Throwable throwable) {
            handleInitializationFailure(throwable);
        }
    }

    /** Visible for tests so startup failure handling can be verified without booting Hazelcast. */
    void initInternal(NodeEngineImpl engine) throws Throwable {
        this.nodeEngine = engine;
        // TODO Determine whether to execute the method on the master node according to the deploy
        // type

        classLoaderService =
                new DefaultClassLoaderService(
                        seaTunnelConfig.getEngineConfig().isClassloaderCacheMode());

        eventService = new EventService(nodeEngine);

        if (EngineConfig.ClusterRole.MASTER_AND_WORKER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            startWorker();
            startMaster();

        } else if (EngineConfig.ClusterRole.WORKER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            startWorker();
        } else {
            startMaster();
        }

        seaTunnelHealthMonitor = new SeaTunnelHealthMonitor(engine.getNode());

        // task log manager service
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs().isEnabled()) {
            taskLogManagerService =
                    new TaskLogManagerService(
                            seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs());
            taskLogManagerService.initClean();
        }

        // Start Jetty server
        if (seaTunnelConfig.getEngineConfig().getHttpConfig().isEnabled()) {
            jettyService = new JettyService(nodeEngine, seaTunnelConfig);
            jettyService.createJettyServer();
        }
    }

    private void startMaster() throws Throwable {
        List<URL> jars = discoverCheckpointStorageJars();
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader = new URLClassLoader(jars.toArray(new URL[0]));
        LOGGER.info("init seatunnel server with " + jars.size() + " jars");

        runWithThreadContextClassLoader(
                classLoader,
                () -> {
                    coordinatorService =
                            new CoordinatorService(
                                    nodeEngine,
                                    this,
                                    seaTunnelConfig.getEngineConfig(),
                                    classLoader);
                    checkpointService =
                            new CheckpointService(
                                    seaTunnelConfig.getEngineConfig().getCheckpointConfig());
                    monitorService = Executors.newSingleThreadScheduledExecutor();
                    monitorService.scheduleAtFixedRate(
                            this::printExecutionInfo,
                            0,
                            seaTunnelConfig.getEngineConfig().getPrintExecutionInfoInterval(),
                            TimeUnit.SECONDS);
                });
    }

    /** Lets storage JAR discovery failures flow through the outer fail-fast handler. */
    List<URL> discoverCheckpointStorageJars() throws IOException {
        String storageType =
                seaTunnelConfig
                        .getEngineConfig()
                        .getCheckpointConfig()
                        .getStorage()
                        .getStoragePluginConfig()
                        .get("storage.type");

        List<URL> jars;
        if (storageType != null && !storageType.trim().isEmpty()) {
            jars =
                    FileUtils.searchJarFilesForStorage(
                            Common.appStarterDir().resolve("zeta"), storageType);
            if (!jars.isEmpty()) {
                LOGGER.info(
                        "Loaded "
                                + jars.size()
                                + " JAR(s) for storage type '"
                                + storageType
                                + "' from starter/zeta");
            }
        } else {
            // load all jars
            jars = FileUtils.searchJarFiles(Common.appStarterDir().resolve("zeta"));
            if (!jars.isEmpty()) {
                LOGGER.info(
                        "Loaded all "
                                + jars.size()
                                + " JAR(s) from starter/zeta (no storage type specified)");
            }
        }
        return jars;
    }

    /** Restores the original thread context ClassLoader on both success and failure paths. */
    void runWithThreadContextClassLoader(ClassLoader classLoader, ThrowingRunnable runnable)
            throws Throwable {
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            runnable.run();
        } finally {
            Thread.currentThread().setContextClassLoader(appClassLoader);
        }
    }

    /** Cleans up partially initialized services before forcing the JVM to exit. */
    void handleInitializationFailure(Throwable throwable) {
        try {
            LOGGER.severe(
                    "SeaTunnel server initialization failed, the server will stop to avoid "
                            + "running in a zombie state. Cause: "
                            + throwable.getMessage(),
                    throwable);
            shutdown(true);
        } catch (Throwable shutdownThrowable) {
            LOGGER.warning(
                    "Failed to clean up partially initialized SeaTunnel server before exit",
                    shutdownThrowable);
        }
        // ServiceManagerImpl swallows exceptions from init(), so we must force-exit to prevent
        // the server from running in a zombie state where Hazelcast is up but the SeaTunnel
        // application layer (e.g. checkpointService, seaTunnelHealthMonitor) was never
        // initialized.
        exitProcess(1);
    }

    /** Separated for tests so fail-fast behavior can be verified without exiting the JVM. */
    void exitProcess(int statusCode) {
        System.exit(statusCode);
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private void startWorker() {
        taskExecutionService =
                new TaskExecutionService(classLoaderService, nodeEngine, eventService);
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(taskExecutionService);
        taskExecutionService.start();
        getSlotService();
    }

    @Override
    public void reset() {}

    @Override
    public void shutdown(boolean terminate) {
        isRunning = false;

        if (jettyService != null) {
            jettyService.shutdownJettyServer();
        }
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

        if (eventService != null) {
            eventService.shutdownNow();
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
            return Boolean.TRUE.equals(
                    RetryUtils.retryWithException(
                            () -> nodeEngine.getThisAddress().equals(nodeEngine.getMasterAddress()),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    exception ->
                                            isRunning && exception instanceof NullPointerException,
                                    Constant.OPERATION_RETRY_SLEEP)));
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

    public ThreadPoolStatus getThreadPoolStatusMetrics() {
        if (coordinatorService == null) {
            return new ThreadPoolStatus(0, 0, 0, 0, 0L, 0L, 0L, 0L);
        }
        return coordinatorService.getThreadPoolStatusMetrics();
    }
}
