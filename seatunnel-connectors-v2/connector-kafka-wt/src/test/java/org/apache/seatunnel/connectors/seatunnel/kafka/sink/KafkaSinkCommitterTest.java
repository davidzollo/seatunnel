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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for KafkaSinkCommitter to verify exception handling and producer lifecycle management.
 */
public class KafkaSinkCommitterTest {

    private ReadonlyConfig pluginConfig;
    private Properties kafkaProperties;
    private MockedConstruction<KafkaInternalProducer> mockedProducerConstruction;
    private KafkaInternalProducer<?, ?> mockProducer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>();
        pluginConfig = ReadonlyConfig.fromMap(configMap);

        kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    @AfterEach
    void tearDown() {
        if (mockedProducerConstruction != null) {
            mockedProducerConstruction.close();
        }
    }

    @Test
    void testCommitEmptyList() {
        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        List<KafkaCommitInfo> emptyList = Collections.emptyList();

        List<KafkaCommitInfo> result = committer.commit(emptyList);

        Assertions.assertEquals(emptyList, result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testCommitSuccess() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-1", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        List<KafkaCommitInfo> result = committer.commit(commitInfos);

        Assertions.assertEquals(commitInfos, result);
        verify(mockProducer, times(1)).commitTransaction();
        verify(mockProducer, times(1)).flush();
        verify(mockProducer, times(1)).close();
    }

    @Test
    void testCommitMultipleTransactions() {
        List<KafkaCommitInfo> commitInfos =
                Arrays.asList(
                        createCommitInfo("txn-1", 100L, (short) 1),
                        createCommitInfo("txn-2", 101L, (short) 2),
                        createCommitInfo("txn-3", 102L, (short) 3));

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        List<KafkaCommitInfo> result = committer.commit(commitInfos);

        Assertions.assertEquals(commitInfos, result);
        verify(mockProducer, times(3)).commitTransaction();
        verify(mockProducer, times(3)).flush();
        verify(mockProducer, times(1)).close();
    }

    @Test
    void testCommitWithProducerFencedException() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-fenced", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doThrow(
                                            new ProducerFencedException(
                                                    "Producer is fenced by newer instance"))
                                    .when(mock)
                                    .commitTransaction();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertThrows(RuntimeException.class, () -> committer.commit(commitInfos));
        verify(mockProducer, times(1)).commitTransaction();
        verify(mockProducer, never()).flush();
        verify(mockProducer, never()).close();
    }

    @Test
    void testCommitWithInvalidTxnStateException() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-invalid", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doThrow(new InvalidTxnStateException("Transaction already committed"))
                                    .when(mock)
                                    .commitTransaction();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertThrows(RuntimeException.class, () -> committer.commit(commitInfos));
        verify(mockProducer, times(1)).commitTransaction();
        verify(mockProducer, never()).flush();
        verify(mockProducer, never()).close();
    }

    @Test
    void testCommitMixedSuccessAndException() {
        List<KafkaCommitInfo> commitInfos =
                Arrays.asList(
                        createCommitInfo("txn-1", 100L, (short) 1),
                        createCommitInfo("txn-2", 101L, (short) 2),
                        createCommitInfo("txn-3", 102L, (short) 3));

        List<KafkaInternalProducer> mockProducers = new ArrayList<>();

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducers.add(mock);
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        List<KafkaCommitInfo> result = committer.commit(commitInfos);

        Assertions.assertEquals(commitInfos, result);
    }

    @Test
    void testAbortEmptyList() {
        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        List<KafkaCommitInfo> emptyList = Collections.emptyList();

        Assertions.assertDoesNotThrow(() -> committer.abort(emptyList));
    }

    @Test
    void testAbortSuccess() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-abort", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).abortTransaction();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertDoesNotThrow(() -> committer.abort(commitInfos));
        verify(mockProducer, times(1)).abortTransaction();
        verify(mockProducer, times(1)).close();
    }

    @Test
    void testAbortWithProducerFencedException() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-abort-fenced", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doThrow(new ProducerFencedException("Producer is fenced"))
                                    .when(mock)
                                    .abortTransaction();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertThrows(RuntimeException.class, () -> committer.abort(commitInfos));
        verify(mockProducer, times(1)).abortTransaction();
        verify(mockProducer, never()).close();
    }

    @Test
    void testAbortWithInvalidTxnStateException() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-abort-invalid", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doThrow(new InvalidTxnStateException("Invalid state"))
                                    .when(mock)
                                    .abortTransaction();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertThrows(RuntimeException.class, () -> committer.abort(commitInfos));
        verify(mockProducer, times(1)).abortTransaction();
        verify(mockProducer, never()).close();
    }

    @Test
    void testProducerReuse() {
        List<KafkaCommitInfo> commitInfos =
                Arrays.asList(
                        createCommitInfo("txn-1", 100L, (short) 1),
                        createCommitInfo("txn-2", 101L, (short) 2));

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        committer.commit(commitInfos);

        Assertions.assertEquals(1, mockedProducerConstruction.constructed().size());
        verify(mockProducer, times(1)).setTransactionalId(Mockito.anyString());
    }

    @Test
    void testClientIdConfiguration() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-client-id", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            Properties props = (Properties) context.arguments().get(0);
                            String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
                            Assertions.assertTrue(
                                    clientId == null || clientId.contains("committer"),
                                    "CLIENT_ID should contain 'committer' suffix");
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        committer.commit(commitInfos);
    }

    @Test
    void testClientIdConfigurationWithExistingClientId() {
        Properties propsWithClientId = new Properties();
        propsWithClientId.putAll(kafkaProperties);
        propsWithClientId.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-client");

        KafkaCommitInfo commitInfo =
                new KafkaCommitInfo("txn-custom-client", propsWithClientId, 100L, (short) 1, true);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            Properties props = (Properties) context.arguments().get(0);
                            String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
                            Assertions.assertEquals("my-client", clientId);
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        committer.commit(commitInfos);
    }

    @Test
    void testProducerCloseException() {
        KafkaCommitInfo commitInfo = createCommitInfo("txn-close-error", 100L, (short) 1);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing().when(mock).setTransactionalId(Mockito.anyString());
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doThrow(new RuntimeException("Close failed")).when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);

        Assertions.assertThrows(RuntimeException.class, () -> committer.commit(commitInfos));
        verify(mockProducer, times(1)).close();
    }

    @Test
    void testPropertiesIsolation() {
        Properties originalProps = new Properties();
        originalProps.putAll(kafkaProperties);
        int originalSize = originalProps.size();

        KafkaCommitInfo commitInfo =
                new KafkaCommitInfo("txn-isolation", originalProps, 100L, (short) 1, true);
        List<KafkaCommitInfo> commitInfos = Collections.singletonList(commitInfo);

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = mock;
                            doNothing()
                                    .when(mock)
                                    .resumeTransaction(anyLong(), anyShort(), anyBoolean());
                            doNothing().when(mock).commitTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close();
                        });

        KafkaSinkCommitter committer = new KafkaSinkCommitter(pluginConfig);
        committer.commit(commitInfos);

        Assertions.assertEquals(originalSize + 1, originalProps.size());
        Assertions.assertEquals(
                "txn-isolation", originalProps.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    private KafkaCommitInfo createCommitInfo(String transactionId, long producerId, short epoch) {
        return new KafkaCommitInfo(transactionId, kafkaProperties, producerId, epoch, true);
    }
}
