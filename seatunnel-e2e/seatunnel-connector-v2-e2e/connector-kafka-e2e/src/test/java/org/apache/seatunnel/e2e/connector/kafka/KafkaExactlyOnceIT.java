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

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Disabled("Temporarily disabled - needs to be fixed")
@Slf4j
public class KafkaExactlyOnceIT extends TestSuiteBase implements TestResource {
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static final String KAFKA_HOST = "kafkaCluster";

    private KafkaContainer kafkaContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started for KafkaExactlyOnceIT");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    @TestTemplate
    public void testSinkKafkaExactlyOncePauseAndResume(TestContainer container)
            throws IOException, InterruptedException {
        // 1) Start streaming job asynchronously
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/kafka/kafka_exactly_once_streaming.conf");
                    } catch (Exception e) {
                        log.error("Execute job exception: {}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // 2) Wait to obtain job id from server logs
        AtomicReference<String> jobId = new AtomicReference<>();
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        Pattern jobIdPattern =
                Pattern.compile(
                        ".*Init JobMaster for Job SeaTunnel_Job \\(([0-9]+)\\).*", Pattern.DOTALL);
        while (System.currentTimeMillis() < deadline && jobId.get() == null) {
            String logs = container.getServerLogs();
            Matcher matcher = jobIdPattern.matcher(logs);
            if (matcher.find()) {
                jobId.set(matcher.group(1));
                break;
            }
            Thread.sleep(500);
        }
        Assertions.assertNotNull(jobId.get(), "JobId should not be null after starting job");

        // 3) Wait some time to ensure at least one checkpoint completed then trigger savepoint
        Thread.sleep(15000);
        String logsBeforeSavepoint = container.getServerLogs();
        Assertions.assertTrue(
                logsBeforeSavepoint.contains("checkpoint is enabled"),
                "Checkpoint should be enabled in streaming job");
        Container.ExecResult savepoint = container.savepointJob(jobId.get());
        Assertions.assertEquals(0, savepoint.getExitCode(), savepoint.getStderr());

        // 4) Restore job from savepoint
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container
                                .restoreJob("/kafka/kafka_exactly_once_streaming.conf", jobId.get())
                                .getExitCode();
                    } catch (Exception e) {
                        log.error("Restore job exception: {}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // 5) Give job a while to run, then assert no InvalidTxnStateException in logs
        Thread.sleep(15000);
        String logsAfterRestore = container.getServerLogs();
        Assertions.assertFalse(
                logsAfterRestore.contains("InvalidTxnStateException"),
                "Logs must not contain InvalidTxnStateException after restore");

        // 6) Sanity check: consume a few records from topic to ensure sink works
        int received = pollRecordsCount("test_txn_topic", 10);
        Assertions.assertTrue(received > 0, "Should be able to consume records from Kafka");
    }

    private int pollRecordsCount(String topic, int seconds) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-exactly-once-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long end = System.currentTimeMillis() + seconds * 1000L;
            int count = 0;
            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    count++;
                }
            }
            return count;
        }
    }
}
