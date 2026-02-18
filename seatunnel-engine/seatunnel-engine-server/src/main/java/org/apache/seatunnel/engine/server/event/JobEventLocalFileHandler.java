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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Writes StainTrace events to local JSONL files in OpenTelemetry OTLP JSON format.
 *
 * <p>Each line is a single {@code ExportTraceServiceRequest} (OTLP JSON) containing one span that
 * represents the end-to-end journey of one sampled row through the pipeline. Pipeline stages are
 * recorded as OTel Span Events with {@code timeUnixNano} timestamps.
 *
 * <p>OTLP JSON reference: https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
 */
@Slf4j
public class JobEventLocalFileHandler implements EventHandler {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    public static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);
    private static final int LOCAL_EVENT_BUFFER_CAPACITY = 2000;
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String baseDir;
    private final int maxEventsPerFile;
    private final long maxFileSizeBytes;
    private final Ringbuffer ringbuffer;
    private volatile long committedEventIndex;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object localBufferLock = new Object();
    private final Deque<Event> localBuffer = new ArrayDeque<>();

    private volatile TraceFileWriter currentWriter;
    private final Object writerLock = new Object();

    public JobEventLocalFileHandler(String baseDir, Ringbuffer ringbuffer) {
        this(baseDir, REPORT_INTERVAL, ringbuffer, 10000, 10 * 1024 * 1024L);
    }

    public JobEventLocalFileHandler(
            String baseDir, Duration reportInterval, Ringbuffer ringbuffer) {
        this(baseDir, reportInterval, ringbuffer, 10000, 10 * 1024 * 1024L);
    }

    public JobEventLocalFileHandler(
            String baseDir,
            Duration reportInterval,
            Ringbuffer ringbuffer,
            int maxEventsPerFile,
            long maxFileSizeBytes) {
        this.baseDir = baseDir;
        this.maxEventsPerFile = maxEventsPerFile;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.ringbuffer = ringbuffer;
        this.committedEventIndex = ringbuffer.headSequence();
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("local-file-report-event-scheduler-%d")
                                .build());
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    try {
                        report();
                    } catch (Throwable e) {
                        log.error("Failed to report event to local file", e);
                    }
                },
                0,
                reportInterval.getSeconds(),
                TimeUnit.SECONDS);
    }

    @Override
    public void handle(Event event) {
        addToLocalBuffer(event);
        try {
            CompletionStage completionStage = ringbuffer.addAsync(event, OverflowPolicy.OVERWRITE);
            completionStage.toCompletableFuture().join();
        } catch (HazelcastInstanceNotActiveException e) {
            log.info("Skip writing event to ringbuffer because Hazelcast instance is not active");
        }
    }

    @VisibleForTesting
    synchronized void report() throws IOException {
        reportFromRingbuffer();
    }

    private boolean reportFromRingbuffer() throws IOException {
        long headSequence = ringbuffer.headSequence();
        if (headSequence > committedEventIndex) {
            log.warn(
                    "The head sequence {} is greater than the committed event index {}",
                    headSequence,
                    committedEventIndex);
            committedEventIndex = headSequence;
        }
        CompletionStage<ReadResultSet<Event>> completionStage =
                ringbuffer.readManyAsync(
                        committedEventIndex, 0, RingbufferProxy.MAX_BATCH_SIZE, null);
        ReadResultSet<Event> resultSet = completionStage.toCompletableFuture().join();
        if (resultSet.size() <= 0) {
            return false;
        }
        if (writeEventsToFile(resultSet)) {
            committedEventIndex += resultSet.readCount();
            drainLocalBuffer(resultSet.readCount());
            return true;
        }
        return false;
    }

    private void reportFromLocalBuffer() throws IOException {
        List<Event> snapshot;
        synchronized (localBufferLock) {
            if (localBuffer.isEmpty()) {
                return;
            }
            snapshot = new ArrayList<>(localBuffer);
        }
        if (writeEventsToFile(snapshot)) {
            synchronized (localBufferLock) {
                localBuffer.clear();
            }
        }
    }

    private boolean writeEventsToFile(Iterable<Event> events) throws IOException {
        synchronized (writerLock) {
            for (Event event : events) {
                if (!(event instanceof StainTraceEvent)) {
                    continue;
                }
                StainTraceEvent traceEvent = (StainTraceEvent) event;
                String jobId = traceEvent.getJobId();
                if (jobId == null || jobId.isEmpty()) {
                    log.warn("Skip event with null or empty jobId");
                    continue;
                }

                String otlpLine;
                try {
                    otlpLine = toOtlpJsonLine(traceEvent);
                } catch (Exception e) {
                    log.warn(
                            "Skip event with invalid payload for traceId={}: {}",
                            traceEvent.getTraceId(),
                            e.getMessage());
                    continue;
                }
                if (otlpLine == null) {
                    continue;
                }

                String date =
                        java.time.Instant.ofEpochMilli(traceEvent.getCreatedTime())
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate()
                                .format(DATE_FORMATTER);

                if (currentWriter == null) {
                    currentWriter = new TraceFileWriter(baseDir, jobId, date);
                } else if (!jobId.equals(currentWriter.getJobId())
                        || !date.equals(currentWriter.getDate())
                        || currentWriter.needsRotation(maxEventsPerFile, maxFileSizeBytes)) {
                    currentWriter.close();
                    currentWriter = new TraceFileWriter(baseDir, jobId, date);
                    log.info("Rotated trace file for job: {}", jobId);
                }
                currentWriter.writeJsonLine(otlpLine);
            }
            if (currentWriter != null) {
                currentWriter.flush();
            }
        }
        return true;
    }

    /**
     * Converts a StainTraceEvent to an OTLP JSON line (one ExportTraceServiceRequest per row).
     *
     * <p>OTel mapping:
     *
     * <ul>
     *   <li>One sampled row → one OTel Span
     *   <li>Each pipeline stage stamp → one Span Event with {@code timeUnixNano}
     *   <li>TraceId: 128-bit = 0x0000000000000000 || traceId (zero-extended from 64-bit)
     *   <li>SpanId: 64-bit = hex(sinkTaskId)
     * </ul>
     */
    private String toOtlpJsonLine(StainTraceEvent event) throws Exception {
        byte[] payload = event.getPayload();
        if (!StainTracePayload.isValid(payload)) {
            return null;
        }

        long traceId = StainTracePayload.readTraceId(payload);
        long startTsMs = StainTracePayload.readStartTsMs(payload);
        List<StainTracePayload.Entry> rawEntries = StainTracePayload.readEntries(payload);

        if (rawEntries.isEmpty()) {
            return null;
        }

        // OTel traceId: 128-bit hex (zero-extend our 64-bit id)
        String traceIdHex = String.format("%016x%016x", 0L, traceId);
        // OTel spanId: 64-bit hex from sinkTaskId
        String spanIdHex = String.format("%016x", event.getSinkTaskId());

        long endTsMs = startTsMs;
        List<Map<String, Object>> otlpEvents = new ArrayList<>();
        for (StainTracePayload.Entry entry : rawEntries) {
            if (entry.tsMs > endTsMs) {
                endTsMs = entry.tsMs;
            }
            String stageName =
                    StainTraceStage.fromCode(entry.stageCode)
                            .map(Enum::name)
                            .orElse("STAGE_" + entry.stageCode);

            Map<String, Object> evt = new LinkedHashMap<>();
            evt.put("name", stageName);
            evt.put("timeUnixNano", String.valueOf(entry.tsMs * 1_000_000L));
            evt.put(
                    "attributes",
                    Arrays.asList(
                            otlpIntAttr("seatunnel.stage_code", entry.stageCode),
                            otlpIntAttr("seatunnel.task_id", entry.taskId)));
            otlpEvents.add(evt);
        }

        // Build Span attributes
        List<Map<String, Object>> spanAttrs =
                Arrays.asList(
                        otlpStrAttr("seatunnel.table_id", event.getTableId()),
                        otlpIntAttr("seatunnel.sink_task_id", event.getSinkTaskId()));

        // Build Span
        Map<String, Object> span = new LinkedHashMap<>();
        span.put("traceId", traceIdHex);
        span.put("spanId", spanIdHex);
        span.put("parentSpanId", "");
        span.put("name", "seatunnel.record");
        span.put("kind", 1); // SPAN_KIND_INTERNAL
        span.put("startTimeUnixNano", String.valueOf(startTsMs * 1_000_000L));
        span.put("endTimeUnixNano", String.valueOf(endTsMs * 1_000_000L));
        span.put("attributes", spanAttrs);
        span.put("events", otlpEvents);
        Map<String, Object> statusMap = new LinkedHashMap<>();
        statusMap.put("code", 1); // STATUS_CODE_OK
        span.put("status", statusMap);

        // Build Resource
        List<Map<String, Object>> resourceAttrs =
                Arrays.asList(
                        otlpStrAttr("service.name", "seatunnel"),
                        otlpStrAttr("seatunnel.job_id", event.getJobId()));

        // Build scope
        Map<String, Object> scopeMap = new LinkedHashMap<>();
        scopeMap.put("name", "seatunnel.stain_trace");

        Map<String, Object> scopeSpanMap = new LinkedHashMap<>();
        scopeSpanMap.put("scope", scopeMap);
        scopeSpanMap.put("spans", Collections.singletonList(span));

        Map<String, Object> resourceMap = new LinkedHashMap<>();
        resourceMap.put("attributes", resourceAttrs);

        Map<String, Object> resourceSpanMap = new LinkedHashMap<>();
        resourceSpanMap.put("resource", resourceMap);
        resourceSpanMap.put("scopeSpans", Collections.singletonList(scopeSpanMap));

        // Build OTLP ExportTraceServiceRequest
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("resourceSpans", Collections.singletonList(resourceSpanMap));

        return JSON_MAPPER.writeValueAsString(root);
    }

    // --- OTLP attribute helpers ---

    private static Map<String, Object> otlpStrAttr(String key, String value) {
        Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("stringValue", value != null ? value : "");
        Map<String, Object> attr = new LinkedHashMap<>();
        attr.put("key", key);
        attr.put("value", valueMap);
        return attr;
    }

    private static Map<String, Object> otlpIntAttr(String key, long value) {
        Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("intValue", String.valueOf(value));
        Map<String, Object> attr = new LinkedHashMap<>();
        attr.put("key", key);
        attr.put("value", valueMap);
        return attr;
    }

    // --- Infrastructure ---

    @Override
    public void close() {
        log.info("Close local file report handler");
        scheduledExecutorService.shutdown();
        try {
            // Wait for any in-flight scheduled report() to finish before we
            // call report() ourselves, so committedEventIndex and currentWriter
            // are not touched concurrently.
            scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            report(); // synchronized — safe after awaitTermination
        } catch (HazelcastInstanceNotActiveException e) {
            // Hazelcast shutting down — drain from local buffer instead
        } catch (IOException e) {
            log.error("Failed to flush events on close", e);
        }
        try {
            reportFromLocalBuffer();
        } catch (IOException e) {
            log.error("Failed to flush events from local buffer on close", e);
        }
        synchronized (writerLock) {
            TraceFileWriter writer = currentWriter;
            currentWriter = null;
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("Failed to close current writer", e);
                }
            }
        }
    }

    private void addToLocalBuffer(Event event) {
        synchronized (localBufferLock) {
            while (localBuffer.size() >= LOCAL_EVENT_BUFFER_CAPACITY) {
                localBuffer.pollFirst();
            }
            localBuffer.addLast(event);
        }
    }

    private void drainLocalBuffer(int count) {
        if (count <= 0) {
            return;
        }
        synchronized (localBufferLock) {
            int remaining = count;
            while (remaining > 0 && !localBuffer.isEmpty()) {
                localBuffer.pollFirst();
                remaining--;
            }
        }
    }

    @VisibleForTesting
    TraceFileWriter getCurrentWriter() {
        return currentWriter;
    }
}
