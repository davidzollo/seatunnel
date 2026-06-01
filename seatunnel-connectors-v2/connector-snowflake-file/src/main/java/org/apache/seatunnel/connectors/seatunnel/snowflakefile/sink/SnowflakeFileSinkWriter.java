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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file.LocalFileWriter;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file.S3FileWriter;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.file.StagingFileWriter;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.state.SnowflakeFileSinkState;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class SnowflakeFileSinkWriter
        implements SinkWriter<SeaTunnelRow, SnowflakeFileCommitInfo, SnowflakeFileSinkState> {

    private final SnowflakeFileConfig config;
    private final SeaTunnelRowType rowType;
    private final StagingFileWriter stagingFileWriter;
    private final String writerId;

    private final ConcurrentMap<String, Long> partitionRowCounts;
    private final ConcurrentMap<String, List<String>> pendingCommitFiles;
    private long pendingCommitRows;
    private long pendingCommitFileCount;

    public SnowflakeFileSinkWriter(
            SnowflakeFileConfig config, SeaTunnelRowType rowType, SinkWriter.Context context) {
        this(config, rowType, context, Collections.emptyList());
    }

    public SnowflakeFileSinkWriter(
            SnowflakeFileConfig config,
            SeaTunnelRowType rowType,
            SinkWriter.Context context,
            List<SnowflakeFileSinkState> states) {
        this.config = config;
        this.rowType = rowType;
        this.writerId =
                context.getIndexOfSubtask() + "-" + UUID.randomUUID().toString().substring(0, 8);

        this.stagingFileWriter = createStagingFileWriter();
        this.partitionRowCounts = new ConcurrentHashMap<>();
        this.pendingCommitFiles = new ConcurrentHashMap<>();
        restorePendingCommit(states);
        log.info("SnowflakeFileSinkWriter initialized for task {}", writerId);
    }

    private StagingFileWriter createStagingFileWriter() {
        if (config.isLocalFileStagingBackend()) {
            return new LocalFileWriter(config, rowType, writerId);
        }

        S3Client s3Client = createS3Client();
        return new S3FileWriter(s3Client, config, rowType);
    }

    /** Create an S3 client for the configured staging backend. */
    private S3Client createS3Client() {
        AwsBasicCredentials awsCreds =
                AwsBasicCredentials.create(
                        config.getAwsAccessKeyId(), config.getAwsSecretAccessKey());

        String regionStr = config.getS3Region();

        S3ClientBuilder builder =
                S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(awsCreds));

        try {
            builder.region(Region.of(regionStr));
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                    "Invalid S3 region: "
                            + regionStr
                            + ". Supported regions include: us-east-1, us-west-2, cn-north-1, cn-northwest-1, etc.",
                    e);
        }

        return builder.build();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (partitionRowCounts.isEmpty()) {
            log.info("First row received by writer {}", writerId);
        }

        String partitionId = calculatePartitionId(element);

        try {
            stagingFileWriter.writeRow(element, partitionId);
            partitionRowCounts.merge(partitionId, 1L, Long::sum);

            long totalRows = partitionRowCounts.values().stream().mapToLong(Long::longValue).sum();
            if (totalRows % 1000 == 0) {
                log.info("Writer {} processed {} rows", writerId, totalRows);
            }
        } catch (IOException e) {
            log.error("Failed to write row to S3 by writer {}", writerId, e);
            throw new IOException("Failed to write row to S3", e);
        }
    }

    /** Calculate the partition id for a row. */
    private String calculatePartitionId(SeaTunnelRow row) {
        int hash = row.hashCode();
        return String.valueOf((hash & Integer.MAX_VALUE) % 10);
    }

    @Override
    public Optional<SnowflakeFileCommitInfo> prepareCommit() throws IOException {
        log.info(
                "Preparing commit for writer {} with total rows: {}",
                writerId,
                partitionRowCounts.values().stream().mapToLong(Long::longValue).sum());

        try {
            stagingFileWriter.flushAll();

            Map<String, List<String>> partitionFiles =
                    mergePartitionFiles(
                            copyPartitionFiles(pendingCommitFiles),
                            stagingFileWriter.getAllUploadedFiles());

            if (partitionFiles.isEmpty()) {
                log.warn("No files to commit for writer {}", writerId);
                return Optional.empty();
            }

            long totalRows =
                    pendingCommitRows
                            + partitionRowCounts.values().stream().mapToLong(Long::longValue).sum();
            long totalFiles = partitionFiles.values().stream().mapToLong(List::size).sum();

            log.info(
                    "Writer {} prepared to commit: {} rows, {} files, partitions: {}",
                    writerId,
                    totalRows,
                    totalFiles,
                    partitionFiles.size());

            pendingCommitFiles.clear();
            pendingCommitFiles.putAll(copyPartitionFiles(partitionFiles));
            pendingCommitRows = totalRows;
            pendingCommitFileCount = totalFiles;
            stagingFileWriter.clearUploadedFiles();
            partitionRowCounts.clear();

            return Optional.of(new SnowflakeFileCommitInfo(partitionFiles, totalRows, totalFiles));

        } catch (Exception e) {
            log.error("Failed to prepare commit for writer {}", writerId, e);
            throw new IOException("Failed to prepare commit", e);
        }
    }

    @Override
    public List<SnowflakeFileSinkState> snapshotState(long checkpointId) {
        if (pendingCommitFiles.isEmpty()) {
            return Collections.emptyList();
        }
        SnowflakeFileSinkState state =
                new SnowflakeFileSinkState(
                        copyPartitionFiles(pendingCommitFiles),
                        pendingCommitRows,
                        pendingCommitFileCount);
        pendingCommitFiles.clear();
        pendingCommitRows = 0L;
        pendingCommitFileCount = 0L;
        return Collections.singletonList(state);
    }

    @Override
    public void abortPrepare() {
        log.warn("Aborting prepare for writer {}", writerId);
        try {
            Map<String, List<String>> allFiles =
                    mergePartitionFiles(
                            copyPartitionFiles(pendingCommitFiles),
                            stagingFileWriter.getAllUploadedFiles());
            for (List<String> files : allFiles.values()) {
                stagingFileWriter.cleanupFiles(files);
            }
            pendingCommitFiles.clear();
            pendingCommitRows = 0L;
            pendingCommitFileCount = 0L;
        } catch (Exception e) {
            log.error("Failed to cleanup files during abort", e);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Closing SnowflakeFileSinkWriter {}", writerId);

        IOException exception = null;

        try {
            stagingFileWriter.close();
        } catch (IOException e) {
            exception = e;
            log.error("Failed to close staging file writer", e);
        }

        if (exception != null) {
            throw exception;
        }
    }

    private void restorePendingCommit(List<SnowflakeFileSinkState> states) {
        if (states == null || states.isEmpty()) {
            return;
        }
        for (SnowflakeFileSinkState state : states) {
            if (state == null || state.getPartitionFiles() == null) {
                continue;
            }
            mergePartitionFiles(pendingCommitFiles, state.getPartitionFiles());
            pendingCommitRows += state.getTotalRows();
            pendingCommitFileCount += state.getTotalFiles();
        }
    }

    private Map<String, List<String>> copyPartitionFiles(Map<String, List<String>> partitionFiles) {
        Map<String, List<String>> copy = new HashMap<>();
        partitionFiles.forEach(
                (partitionId, files) -> copy.put(partitionId, new ArrayList<>(files)));
        return copy;
    }

    private Map<String, List<String>> mergePartitionFiles(
            Map<String, List<String>> target, Map<String, List<String>> source) {
        source.forEach(
                (partitionId, files) ->
                        target.computeIfAbsent(partitionId, ignored -> new ArrayList<>())
                                .addAll(files));
        return target;
    }
}
