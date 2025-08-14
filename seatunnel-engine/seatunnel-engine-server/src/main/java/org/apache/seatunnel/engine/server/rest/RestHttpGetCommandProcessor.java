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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.telemetry.log.LogoutService;
import org.apache.seatunnel.engine.server.telemetry.log.operation.PackageJobLogsOperation;
import org.apache.seatunnel.engine.server.telemetry.log.operation.PackageZetaLogsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_200;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.PACKAGE_ALL_LOGS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.PACKAGE_JOB_LOGS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final Log4j2HttpGetCommandProcessor original;

    private static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";

    private static final String SINK_WRITE_COUNT = "SinkWriteCount";

    private NodeEngine nodeEngine;

    public RestHttpGetCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new Log4j2HttpGetCommandProcessor(textCommandService));
    }

    public RestHttpGetCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpGetCommandProcessor log4j2HttpGetCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = log4j2HttpGetCommandProcessor;
    }

    @Override
    public void handle(HttpGetCommand httpGetCommand) {
        String uri = httpGetCommand.getURI();
        try {
            if (uri.startsWith(RUNNING_JOBS_URL)) {
                handleRunningJobsInfo(httpGetCommand);
            } else if (uri.startsWith(FINISHED_JOBS_INFO)) {
                handleFinishedJobsInfo(httpGetCommand, uri);
            } else if (uri.startsWith(RUNNING_JOB_URL)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
            } else if (uri.startsWith(PACKAGE_JOB_LOGS_URL)) {
                handlePackageJobLogs(httpGetCommand, uri);
            } else if (uri.startsWith(PACKAGE_ALL_LOGS_URL)) {
                handlePackageZetaLogs(httpGetCommand, uri);
            } else {
                original.handle(httpGetCommand);
            }
        } catch (IndexOutOfBoundsException e) {
            httpGetCommand.send400();
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + httpGetCommand, e);
            prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
        }

        this.textCommandService.sendResponse(httpGetCommand);
    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    private void getSystemMonitoringInformation(HttpGetCommand command) {
        Cluster cluster = textCommandService.getNode().hazelcastInstance.getCluster();
        nodeEngine = textCommandService.getNode().hazelcastInstance.node.nodeEngine;

        Set<Member> members = cluster.getMembers();
        JsonArray jsonValues =
                members.stream()
                        .map(
                                member -> {
                                    Address address = member.getAddress();
                                    String input = null;
                                    try {
                                        input =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMemberNode(
                                                                        nodeEngine,
                                                                        new GetClusterHealthMetricsOperation(),
                                                                        address)
                                                                .get();
                                    } catch (InterruptedException | ExecutionException e) {
                                        logger.severe("get system monitoring information fail", e);
                                    }
                                    String[] parts = input.split(", ");
                                    JsonObject jobInfo = new JsonObject();
                                    Arrays.stream(parts)
                                            .forEach(
                                                    part -> {
                                                        String[] keyValue = part.split("=");
                                                        jobInfo.add(keyValue[0], keyValue[1]);
                                                    });
                                    return jobInfo;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        this.prepareResponse(command, jsonValues);
    }

    private void handleRunningJobsInfo(HttpGetCommand command) {
        IMap<Long, JobInfo> values =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JsonArray jobs =
                values.entrySet().stream()
                        .map(
                                jobInfoEntry ->
                                        convertToJson(
                                                jobInfoEntry.getValue(), jobInfoEntry.getKey()))
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        this.prepareResponse(command, jobs);
    }

    private void handleFinishedJobsInfo(HttpGetCommand command, String uri) {

        uri = StringUtil.stripTrailingSlash(uri);

        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String state;
        if (indexEnd == -1) {
            state = "";
        } else {
            state = uri.substring(indexEnd + 1);
        }

        IMap<Long, JobState> finishedJob =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_STATE);

        IMap<Long, JobMetrics> finishedJobMetrics =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_METRICS);

        IMap<Long, JobDAGInfo> finishedJobDAGInfo =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);

        JsonArray jobs =
                finishedJob.values().stream()
                        .filter(
                                jobState -> {
                                    if (state.isEmpty()) {
                                        return true;
                                    }
                                    return jobState.getJobStatus()
                                            .name()
                                            .equals(state.toUpperCase());
                                })
                        .sorted(Comparator.comparing(JobState::getFinishTime))
                        .map(
                                jobState -> {
                                    Long jobId = jobState.getJobId();
                                    SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
                                    String jobMetrics;
                                    if (seaTunnelServer == null) {
                                        jobMetrics =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMasterNode(
                                                                        getNode().nodeEngine,
                                                                        new GetJobMetricsOperation(
                                                                                jobId))
                                                                .join();
                                    } else {
                                        jobMetrics =
                                                seaTunnelServer
                                                        .getCoordinatorService()
                                                        .getJobMetrics(jobId)
                                                        .toJsonString();
                                    }

                                    JobDAGInfo jobDAGInfo = finishedJobDAGInfo.get(jobId);

                                    return convertToJson(
                                            jobState,
                                            jobMetrics,
                                            Json.parse(JsonUtils.toJsonString(jobDAGInfo))
                                                    .asObject(),
                                            jobId);
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);

        this.prepareResponse(command, jobs);
    }

    private void handleJobInfoById(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String jobId = uri.substring(indexEnd + 1);

        JobInfo jobInfo =
                (JobInfo)
                        this.textCommandService
                                .getNode()
                                .getNodeEngine()
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_RUNNING_JOB_INFO)
                                .get(Long.valueOf(jobId));

        if (!jobId.isEmpty() && jobInfo != null) {
            this.prepareResponse(command, convertToJson(jobInfo, Long.parseLong(jobId)));
        } else {
            this.prepareResponse(command, new JsonObject());
        }
    }

    private Map<String, Long> getJobMetrics(String jobMetrics) {
        Map<String, Long> metricsMap = new HashMap<>();
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        try {
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);
            JsonNode sourceReceivedCountJson = jobMetricsStr.get(SOURCE_RECEIVED_COUNT);
            JsonNode sinkWriteCountJson = jobMetricsStr.get(SINK_WRITE_COUNT);
            for (int i = 0; i < jobMetricsStr.get(SOURCE_RECEIVED_COUNT).size(); i++) {
                JsonNode sourceReader = sourceReceivedCountJson.get(i);
                JsonNode sinkWriter = sinkWriteCountJson.get(i);
                sourceReadCount += sourceReader.get("value").asLong();
                sinkWriteCount += sinkWriter.get("value").asLong();
            }
        } catch (JsonProcessingException | NullPointerException e) {
            return metricsMap;
        }
        metricsMap.put(SOURCE_RECEIVED_COUNT, sourceReadCount);
        metricsMap.put(SINK_WRITE_COUNT, sinkWriteCount);

        return metricsMap;
    }

    private SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                this.textCommandService.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (!seaTunnelServer.isMasterNode() && shouldBeMaster) {
            return null;
        }
        return seaTunnelServer;
    }

    private JsonObject convertToJson(JobInfo jobInfo, long jobId) {

        JsonObject jobInfoJson = new JsonObject();
        JobImmutableInformation jobImmutableInformation =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getSerializationService()
                        .toObject(
                                this.textCommandService
                                        .getNode()
                                        .getNodeEngine()
                                        .getSerializationService()
                                        .toObject(jobInfo.getJobImmutableInformation()));

        ClassLoaderService classLoaderService = getSeaTunnelServer(false).getClassLoaderService();
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        jobId, jobImmutableInformation.getPluginJarsUrls());
        LogicalDag logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        this.textCommandService.getNode().getNodeEngine().getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());
        classLoaderService.releaseClassLoader(jobId, jobImmutableInformation.getPluginJarsUrls());

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        String jobMetrics;
        JobStatus jobStatus;
        if (seaTunnelServer == null) {
            jobMetrics =
                    (String)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            getNode().nodeEngine, new GetJobMetricsOperation(jobId))
                                    .join();
            jobStatus =
                    JobStatus.values()[
                            (int)
                                    NodeEngineUtil.sendOperationToMasterNode(
                                                    getNode().nodeEngine,
                                                    new GetJobStatusOperation(jobId))
                                            .join()];
        } else {
            jobMetrics =
                    seaTunnelServer.getCoordinatorService().getJobMetrics(jobId).toJsonString();
            jobStatus = seaTunnelServer.getCoordinatorService().getJobStatus(jobId);
        }

        jobInfoJson
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, logicalDag.getJobConfig().getName())
                .add(RestConstant.JOB_STATUS, jobStatus.toString())
                .add(
                        RestConstant.ENV_OPTIONS,
                        JsonUtil.toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                .add(
                        RestConstant.CREATE_TIME,
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                .format(new Date(jobImmutableInformation.getCreateTime())))
                .add(RestConstant.JOB_DAG, logicalDag.getLogicalDagAsJson())
                .add(
                        RestConstant.PLUGIN_JARS_URLS,
                        (JsonValue)
                                jobImmutableInformation.getPluginJarsUrls().stream()
                                        .map(
                                                url -> {
                                                    JsonObject jarUrl = new JsonObject();
                                                    jarUrl.add(
                                                            RestConstant.JAR_PATH, url.toString());
                                                    return jarUrl;
                                                })
                                        .collect(JsonArray::new, JsonArray::add, JsonArray::add))
                .add(
                        RestConstant.IS_START_WITH_SAVE_POINT,
                        jobImmutableInformation.isStartWithSavePoint())
                .add(RestConstant.METRICS, JsonUtil.toJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }

    private JsonObject convertToJson(
            JobState jobState, String jobMetrics, JsonObject jobDAGInfo, long jobId) {
        JsonObject jobInfoJson = new JsonObject();
        jobInfoJson
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, jobState.getJobName())
                .add(RestConstant.JOB_STATUS, jobState.getJobStatus().toString())
                .add(RestConstant.ERROR_MSG, jobState.getErrorMessage())
                .add(
                        RestConstant.CREATE_TIME,
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                .format(new Date(jobState.getSubmitTime())))
                .add(
                        RestConstant.FINISH_TIME,
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                .format(new Date(jobState.getFinishTime())))
                .add(RestConstant.JOB_DAG, jobDAGInfo)
                .add(RestConstant.METRICS, JsonUtil.toJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }

    private void handlePackageJobLogs(HttpGetCommand command, String uri) {
        try {
            String jobIdStr = uri.substring(PACKAGE_JOB_LOGS_URL.length() + 1);
            Long jobId = Long.parseLong(jobIdStr);

            SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
            byte[] zipBytes;
            if (seaTunnelServer == null) {
                zipBytes =
                        (byte[])
                                NodeEngineUtil.sendOperationToMasterNode(
                                                getNode().nodeEngine,
                                                new PackageJobLogsOperation(jobId))
                                        .join();
            } else {
                LogoutService logOutService = new LogoutService(seaTunnelServer);
                zipBytes = logOutService.packageJobLogs(jobId);
            }

            String fileName =
                    String.format(
                            "job_%d_logs_%s.zip",
                            jobId, new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
            Map<String, Object> headers = new HashMap<>();
            headers.put("Content-Type", "application/zip");
            headers.put("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
            command.setResponseWithHeaders(SC_200, headers, zipBytes);
        } catch (Exception e) {
            logger.severe("Failed to package job logs", e);
            prepareResponse(SC_500, command, "Failed to package job logs: " + e.getMessage());
            textCommandService.sendResponse(command);
        }
    }

    private void handlePackageZetaLogs(HttpGetCommand command, String uri) {
        try {
            String[] params = uri.substring(PACKAGE_ALL_LOGS_URL.length()).split("\\?");
            String dateParam = null;
            String hostParam = null;
            if (params.length > 1) {
                String[] queryParams = params[1].split("&");
                for (String param : queryParams) {
                    String[] keyValue = param.split("=");
                    if (keyValue.length == 2) {
                        if ("date".equals(keyValue[0])) {
                            dateParam = keyValue[1];
                        } else if ("host".equals(keyValue[0])) {
                            hostParam = keyValue[1];
                        }
                    }
                }
            }

            PackageZetaLogsOperation operation;

            if (dateParam != null && !dateParam.trim().isEmpty()) {
                if (hostParam != null && !hostParam.trim().isEmpty()) {
                    operation = new PackageZetaLogsOperation(dateParam, hostParam);
                } else {
                    operation = new PackageZetaLogsOperation(dateParam);
                }
            } else {
                if (hostParam != null && !hostParam.trim().isEmpty()) {
                    operation = new PackageZetaLogsOperation(LocalDate.now(), hostParam);
                } else {
                    operation = new PackageZetaLogsOperation();
                }
            }

            SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
            byte[] zipBytes;
            if (seaTunnelServer == null) {
                zipBytes =
                        (byte[])
                                NodeEngineUtil.sendOperationToMasterNode(
                                                getNode().nodeEngine, operation)
                                        .join();
            } else {
                LogoutService logOutService = new LogoutService(seaTunnelServer);
                if (dateParam != null && !dateParam.trim().isEmpty()) {
                    try {
                        LocalDate date =
                                LocalDate.parse(
                                        dateParam, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        if (hostParam != null && !hostParam.trim().isEmpty()) {
                            zipBytes = logOutService.packageZetaLogs(date, hostParam);
                        } else {
                            zipBytes = logOutService.packageZetaLogs(date);
                        }
                    } catch (Exception e) {
                        logger.warning(
                                "Invalid date format: " + dateParam + ", using current date", e);
                        if (hostParam != null && !hostParam.trim().isEmpty()) {
                            zipBytes = logOutService.packageZetaLogs(LocalDate.now(), hostParam);
                        } else {
                            zipBytes = logOutService.packageZetaLogs();
                        }
                    }
                } else {
                    if (hostParam != null && !hostParam.trim().isEmpty()) {
                        zipBytes = logOutService.packageZetaLogs(LocalDate.now(), hostParam);
                    } else {
                        zipBytes = logOutService.packageZetaLogs();
                    }
                }
            }

            String fileName =
                    String.format(
                            "all_logs_%s.zip",
                            new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
            Map<String, Object> headers = new HashMap<>();
            headers.put("Content-Type", "application/zip");
            headers.put("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
            command.setResponseWithHeaders(SC_200, headers, zipBytes);
        } catch (Exception e) {
            logger.severe("Failed to package all logs", e);
            prepareResponse(SC_500, command, "Failed to package all logs: " + e.getMessage());
            textCommandService.sendResponse(command);
        }
    }
}
