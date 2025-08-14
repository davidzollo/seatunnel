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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner.JobMetricsSummary;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetClusterHealthMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPrintMessageCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelRefreshLicenseCodec;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import lombok.Getter;
import lombok.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.common.Constants.FAKE_HOST;

public class SeaTunnelClient implements SeaTunnelClientInstance, AutoCloseable {
    private final SeaTunnelHazelcastClient hazelcastClient;
    @Getter private final JobClient jobClient;

    public SeaTunnelClient(@NonNull ClientConfig clientConfig) {
        this.hazelcastClient = new SeaTunnelHazelcastClient(clientConfig);
        this.jobClient = new JobClient(this.hazelcastClient);
    }

    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, hazelcastClient, seaTunnelConfig);
    }

    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, hazelcastClient, seaTunnelConfig, false, jobId);
    }

    @Override
    public ClientJobExecutionEnvironment restoreExecutionContext(
            @NonNull String filePath,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, hazelcastClient, seaTunnelConfig, true, jobId);
    }

    @Override
    public JobClient createJobClient() {
        return new JobClient(hazelcastClient);
    }

    @Override
    public void close() {
        hazelcastClient.getHazelcastInstance().shutdown();
    }

    public ILogger getLogger() {
        return hazelcastClient.getLogger(getClass());
    }

    public String printMessageToMaster(@NonNull String msg) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelPrintMessageCodec.encodeRequest(msg),
                SeaTunnelPrintMessageCodec::decodeResponse);
    }

    public void refreshMasterLicense() {
        PassiveCompletableFuture<Void> passiveCompletableFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelRefreshLicenseCodec.encodeRequest());
        passiveCompletableFuture.join();
    }

    /**
     * get job status and the tasks status
     *
     * @param jobId jobId
     */
    @Deprecated
    public String getJobDetailStatus(Long jobId) {
        return jobClient.getJobDetailStatus(jobId);
    }

    /** list all jobId and job status */
    @Deprecated
    public String listJobStatus() {
        return jobClient.listJobStatus(false);
    }

    /**
     * get one job status
     *
     * @param jobId jobId
     */
    @Deprecated
    public String getJobStatus(Long jobId) {
        return jobClient.getJobStatus(jobId);
    }

    @Deprecated
    public String getJobMetrics(Long jobId) {
        return jobClient.getJobMetrics(jobId);
    }

    @Deprecated
    public void savePointJob(Long jobId) {
        jobClient.savePointJob(jobId);
    }

    @Deprecated
    public void cancelJob(Long jobId) {
        jobClient.cancelJob(jobId);
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return jobClient.getJobInfo(jobId);
    }

    public JobMetricsSummary getJobMetricsSummary(Long jobId) {
        return jobClient.getJobMetricsSummary(jobId);
    }

    public String getHealthMetrics(String address) {
        Set<Member> members = hazelcastClient.getHazelcastInstance().getCluster().getMembers();
        Member member =
                members.stream()
                        .filter(
                                m ->
                                        (m.getAddress().getHost() + ":" + m.getAddress().getPort())
                                                .equals(address))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Member with address "
                                                        + address
                                                        + " not found in the cluster."));

        return getMetricsByMember(member);
    }

    private String getMetricsByMember(Member member) {
        Map<String, String> kvMap = new LinkedHashMap<>();
        if (FAKE_HOST.equals(member.getAddress().getHost())) {
            return JsonUtils.toJsonString(kvMap);
        }
        String metrics =
                hazelcastClient.requestAndDecodeResponse(
                        member.getUuid(),
                        SeaTunnelGetClusterHealthMetricsCodec.encodeRequest(),
                        SeaTunnelGetClusterHealthMetricsCodec::decodeResponse);
        String[] split = metrics.split(",");
        Arrays.stream(split)
                .forEach(
                        kv -> {
                            String[] kvArr = kv.split("=");
                            kvMap.put(kvArr[0], kvArr[1]);
                        });
        return JsonUtils.toJsonString(kvMap);
    }

    public Map<String, String> getClusterHealthMetrics() {
        Set<Member> members = hazelcastClient.getHazelcastInstance().getCluster().getMembers();
        Map<String, String> healthMetricsMap = new HashMap<>();
        members.forEach(
                member -> {
                    healthMetricsMap.put(
                            member.getAddress().toString(), getMetricsByMember(member));
                });

        return healthMetricsMap;
    }

    public byte[] packageJobLogs(Long jobId) {
        return jobClient.packageJobLogs(jobId);
    }

    /**
     * Pack all log files for the specified date
     *
     * @param dateStr date string in yyyy-MM-dd
     * @return byte array of the packed zip file
     */
    public byte[] packageAllLogs(String dateStr) {
        return this.packageAllLogs(dateStr, null);
    }

    /**
     * Pack all log files for the specified date and host
     *
     * @param dateStr date string in yyyy-MM-dd
     * @param host target host, if null or empty, get logs from all nodes
     * @return byte array of the packed zip file
     */
    public byte[] packageAllLogs(String dateStr, String host) {
        return jobClient.packageZetaLogs(dateStr, host);
    }
}
