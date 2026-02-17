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

package org.apache.seatunnel.trace.analyzer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.trace.analyzer.model.TraceEntry;
import org.apache.seatunnel.trace.analyzer.model.TraceRecord;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class TraceFileReader {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<Integer, String> STAGE_NAMES = new HashMap<>();

    static {
        STAGE_NAMES.put(1, "SOURCE_EMIT");
        STAGE_NAMES.put(2, "QUEUE_IN");
        STAGE_NAMES.put(3, "QUEUE_OUT");
        STAGE_NAMES.put(4, "TRANSFORM_IN");
        STAGE_NAMES.put(5, "TRANSFORM_OUT");
        STAGE_NAMES.put(6, "SINK_WRITE_DONE");
    }

    public List<TraceRecord> readTraces(String baseDir, String jobId, String date)
            throws IOException {
        Path searchPath = buildSearchPath(baseDir, jobId, date);

        if (!Files.exists(searchPath)) {
            log.warn("Search path does not exist: {}", searchPath);
            return new ArrayList<>();
        }

        List<TraceRecord> records = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(searchPath)) {
            paths.filter(p -> p.toString().endsWith(".jsonl"))
                    .forEach(
                            p -> {
                                try {
                                    readFile(p, records);
                                } catch (Exception e) {
                                    log.error("Failed to read file: " + p, e);
                                }
                            });
        }

        log.info("Read {} trace records from {}", records.size(), searchPath);
        return records;
    }

    private void readFile(Path filePath, List<TraceRecord> records) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    StainTraceEvent event = MAPPER.readValue(line, StainTraceEvent.class);
                    TraceRecord record = parseTraceRecord(event);
                    if (record != null) {
                        records.add(record);
                    }
                } catch (Exception e) {
                    log.warn(
                            "Skip invalid JSON at {}:{} - {}",
                            filePath.getFileName(),
                            lineNumber,
                            e.getMessage());
                }
            }
        }
    }

    private TraceRecord parseTraceRecord(StainTraceEvent event) {
        List<TraceEntry> entries = new ArrayList<>();

        if (event.getSpans() != null && !event.getSpans().isEmpty()) {
            JsonNode spanNode = MAPPER.valueToTree(event.getSpans().get(0));
            JsonNode eventsNode = spanNode.get("events");
            if (eventsNode != null && eventsNode.isArray()) {
                for (JsonNode eventNode : eventsNode) {
                    JsonNode attrs = eventNode.get("attributes");
                    if (attrs != null) {
                        int stage = attrs.path("seatunnel.stage_code").asInt(0);
                        long taskId = attrs.path("seatunnel.task_id").asLong(0);
                        String timestamp = eventNode.path("timestamp").asText();
                        long timestampMs = parseTimestamp(timestamp);

                        String stageName = eventNode.path("name").asText();
                        if (stageName == null || stageName.isEmpty()) {
                            stageName = STAGE_NAMES.getOrDefault(stage, "UNKNOWN_" + stage);
                        }
                        entries.add(new TraceEntry(stage, taskId, timestampMs, stageName));
                    }
                }
            }
        }

        if (entries.isEmpty()) {
            return null;
        }

        return new TraceRecord(
                event.getTraceId(),
                event.getSinkTaskId(),
                event.getJobId(),
                event.getTableId(),
                event.getCreatedTime(),
                entries);
    }

    private long parseTimestamp(String iso8601) {
        try {
            return java.time.Instant.parse(iso8601).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private Path buildSearchPath(String baseDir, String jobId, String date) {
        Path path = Paths.get(baseDir, "traces");
        if (jobId != null && !jobId.isEmpty()) {
            path = path.resolve(jobId);
        }
        if (date != null && !date.isEmpty()) {
            path = path.resolve(date);
        }
        return path;
    }
}
