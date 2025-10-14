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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.client.util.ContentFormatUtil;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobStatusData;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobCheckpointCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobDetailStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobInfoCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetRunningJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobsByStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPackageJobLogsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPackageZetaLogsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSavePointJobCodec;

import lombok.NonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class JobClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final SeaTunnelHazelcastClient hazelcastClient;

    public JobClient(@NonNull SeaTunnelHazelcastClient hazelcastClient) {
        this.hazelcastClient = hazelcastClient;
    }

    public long getNewJobId() {
        return hazelcastClient
                .getHazelcastInstance()
                .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                .newId();
    }

    public ClientJobProxy createJobProxy(@NonNull JobImmutableInformation jobImmutableInformation) {
        return new ClientJobProxy(hazelcastClient, jobImmutableInformation);
    }

    public ClientJobProxy getJobProxy(@NonNull Long jobId) {
        return new ClientJobProxy(hazelcastClient, jobId);
    }

    public String getJobDetailStatus(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobDetailStatusCodec.encodeRequest(jobId),
                SeaTunnelGetJobDetailStatusCodec::decodeResponse);
    }

    /** list all jobId and job status */
    public String listJobStatus(boolean format) {
        String jobStatusStr =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelListJobStatusCodec.encodeRequest(),
                        SeaTunnelListJobStatusCodec::decodeResponse);
        if (!format) {
            return jobStatusStr;
        } else {
            try {
                List<JobStatusData> statusDataList =
                        OBJECT_MAPPER.readValue(
                                jobStatusStr, new TypeReference<List<JobStatusData>>() {});
                statusDataList.sort(
                        (s1, s2) -> {
                            if (s1.getSubmitTime() == s2.getSubmitTime()) {
                                return 0;
                            }
                            return s1.getSubmitTime() > s2.getSubmitTime() ? -1 : 1;
                        });
                return ContentFormatUtil.format(statusDataList);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get one job status
     *
     * @param jobId jobId
     */
    public String getJobStatus(Long jobId) {
        int jobStatusOrdinal =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
                        SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal].toString();
    }

    public String getJobMetrics(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobMetricsCodec.encodeRequest(jobId),
                SeaTunnelGetJobMetricsCodec::decodeResponse);
    }

    public String getRunningJobMetrics() {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetRunningJobMetricsCodec.encodeRequest(),
                SeaTunnelGetRunningJobMetricsCodec::decodeResponse);
    }

    /**
     * Get jobs by specific status
     *
     * @param jobStatus the job status to filter by
     * @return JSON string containing list of jobs with the specified status
     */
    public List<JobStatusData> listJobsByStatus(JobStatus jobStatus) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelListJobsByStatusCodec.encodeRequest(jobStatus.toString()),
                SeaTunnelListJobsByStatusCodec::decodeResponse);
    }

    public void savePointJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelSavePointJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public void cancelJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelCancelJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return hazelcastClient
                .getSerializationService()
                .toObject(
                        hazelcastClient.requestOnMasterAndDecodeResponse(
                                SeaTunnelGetJobInfoCodec.encodeRequest(jobId),
                                SeaTunnelGetJobInfoCodec::decodeResponse));
    }

    public JobMetricsRunner.JobMetricsSummary getJobMetricsSummary(Long jobId) {
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        String jobMetrics = getJobMetrics(jobId);
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jobMetrics);
            JsonNode sourceReaders = jsonNode.get("SourceReceivedCount");
            JsonNode sinkWriters = jsonNode.get("SinkWriteCount");
            for (int i = 0; i < sourceReaders.size(); i++) {
                JsonNode sourceReader = sourceReaders.get(i);
                JsonNode sinkWriter = sinkWriters.get(i);
                sourceReadCount += sourceReader.get("value").asLong();
                sinkWriteCount += sinkWriter.get("value").asLong();
            }
            return new JobMetricsRunner.JobMetricsSummary(sourceReadCount, sinkWriteCount);
            // Add NullPointerException because of metrics information can be empty like {}
        } catch (JsonProcessingException | NullPointerException e) {
            return new JobMetricsRunner.JobMetricsSummary(sourceReadCount, sinkWriteCount);
        }
    }

    public List<JobPipelineCheckpointData> getCheckpointData(Long jobId) {
        return hazelcastClient
                .getSerializationService()
                .toObject(
                        hazelcastClient.requestOnMasterAndDecodeResponse(
                                SeaTunnelGetJobCheckpointCodec.encodeRequest(jobId),
                                SeaTunnelGetJobCheckpointCodec::decodeResponse));
    }

    public byte[] packageJobLogs(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelPackageJobLogsCodec.encodeRequest(jobId, false),
                SeaTunnelPackageJobLogsCodec::decodeResponse);
    }

    public byte[] packageZetaLogs(String dateStr, String host) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelPackageZetaLogsCodec.encodeRequest(dateStr, host, false),
                SeaTunnelPackageZetaLogsCodec::decodeResponse);
    }

    /**
     * Get job log content object by jobId
     *
     * @param jobId the job ID
     * @return JobLogContent object containing host and log information
     * @throws IOException if failed to read log content
     */
    public JobLogContent getJobLogContent(Long jobId) throws IOException {
        byte[] logBytes = packageJobLogs(jobId);
        return parseJobLogContent(logBytes, jobId);
    }

    /**
     * Parse job log content from byte array
     *
     * @param logBytes the log bytes from packageJobLogs
     * @param jobId the job ID
     * @return JobLogContent object
     * @throws IOException if failed to parse log content
     */
    private JobLogContent parseJobLogContent(byte[] logBytes, Long jobId) throws IOException {
        List<JobLogContent.NodeLogEntry> nodeLogs = new ArrayList<>();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(logBytes);
                ZipInputStream zis = new ZipInputStream(bais)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();

                if (entryName.contains("node_") && entryName.contains("job_" + jobId)) {
                    String host = extractHostFromEntryName(entryName);
                    String logContent = readZipEntryContent(zis);
                    nodeLogs.add(new JobLogContent.NodeLogEntry(host, logContent));
                }

                zis.closeEntry();
            }
        }

        return new JobLogContent(nodeLogs);
    }

    /**
     * Extract host information from zip entry name
     *
     * @param entryName the zip entry name
     * @return host string
     */
    private String extractHostFromEntryName(String entryName) {
        if (entryName.contains("node_")) {
            String nodePart = entryName.substring(entryName.indexOf("node_") + 5);
            int slashIndex = nodePart.indexOf("/");
            if (slashIndex > 0) {
                return nodePart.substring(0, slashIndex);
            }
            return nodePart;
        }
        return "unknown";
    }

    /**
     * Read content from zip entry
     *
     * @param zis the zip input stream
     * @return content as string
     * @throws IOException if failed to read content
     */
    private String readZipEntryContent(ZipInputStream zis) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = zis.read(buffer)) > 0) {
            baos.write(buffer, 0, length);
        }

        byte[] content = baos.toByteArray();

        // Check if the content is a zip file (starts with PK magic number)
        if (isZipFile(content)) {
            // If it's a zip file, recursively extract the first text file
            return extractTextFromZip(content);
        } else {
            // If it's not a zip file, return as string
            return baos.toString(StandardCharsets.UTF_8.name());
        }
    }

    /**
     * Check if byte array represents a zip file by checking the magic number
     *
     * @param content the byte array to check
     * @return true if it's a zip file, false otherwise
     */
    private boolean isZipFile(byte[] content) {
        // ZIP file magic number: PK (0x50, 0x4B)
        return content.length >= 2 && content[0] == 0x50 && content[1] == 0x4B;
    }

    /**
     * Extract text content from a zip file
     *
     * @param zipContent the zip file content as byte array
     * @return extracted text content
     * @throws IOException if failed to extract content
     */
    private String extractTextFromZip(byte[] zipContent) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(zipContent);
                ZipInputStream zis = new ZipInputStream(bais)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                // Skip directories
                if (!entry.isDirectory()) {
                    // Read the first file found (assuming it's a text file)
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = zis.read(buffer)) > 0) {
                        baos.write(buffer, 0, length);
                    }
                    zis.closeEntry();
                    return baos.toString(StandardCharsets.UTF_8.name());
                }
                zis.closeEntry();
            }
        }

        // If no text file found, return empty string
        return "";
    }
}
