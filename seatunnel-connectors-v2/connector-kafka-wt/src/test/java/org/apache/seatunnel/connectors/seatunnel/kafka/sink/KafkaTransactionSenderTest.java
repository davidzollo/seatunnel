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

import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for KafkaTransactionSender to verify producer reuse and transaction management. */
class KafkaTransactionSenderTest {

    private Properties kafkaProperties;
    private String transactionPrefix;
    private MockedConstruction<KafkaInternalProducer> mockedProducerConstruction;
    private KafkaInternalProducer<String, String> mockProducer;

    @BeforeEach
    void setUp() {
        kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        transactionPrefix = "test-txn-prefix";
    }

    @AfterEach
    void tearDown() {
        if (mockedProducerConstruction != null) {
            mockedProducerConstruction.close();
        }
    }

    @Test
    void testBeginTransaction() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");

        Assertions.assertEquals(1, mockedProducerConstruction.constructed().size());
        verify(mockProducer, times(1)).initTransactions();
        verify(mockProducer, times(1)).beginTransaction();
    }

    @Test
    void testSend() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            when(mock.send(any(ProducerRecord.class))).thenReturn(null);
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        ProducerRecord<String, String> record =
                new ProducerRecord<>("test-topic", "key1", "value1");
        sender.send(record);

        verify(mockProducer, times(1)).send(record);
    }

    @Test
    void testPrepareCommit() {
        long expectedProducerId = 12345L;
        short expectedEpoch = 1;
        boolean expectedTxnStarted = true;

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            when(mock.getProducerId()).thenReturn(expectedProducerId);
                            when(mock.getEpoch()).thenReturn(expectedEpoch);
                            when(mock.isTxnStarted()).thenReturn(expectedTxnStarted);
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        String transactionId = "txn-001";
        sender.beginTransaction(transactionId);
        Optional<KafkaCommitInfo> commitInfo = sender.prepareCommit();

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(transactionId, commitInfo.get().getTransactionId());
        Assertions.assertEquals(expectedProducerId, commitInfo.get().getProducerId());
        Assertions.assertEquals(expectedEpoch, commitInfo.get().getEpoch());
        Assertions.assertEquals(kafkaProperties, commitInfo.get().getKafkaProperties());
    }

    @Test
    void testAbortTransaction() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).abortTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.abortTransaction();

        verify(mockProducer, times(1)).abortTransaction();
    }

    @Test
    void testAbortTransactionWithCheckpointId() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactionId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).flush();
                            when(mock.getEpoch()).thenReturn((short) 1).thenReturn((short) 0);
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.abortTransaction(100L);

        verify(mockProducer, times(2)).initTransactionId(anyString());
        verify(mockProducer, times(2)).flush();
        verify(mockProducer, times(3)).getEpoch();
    }

    @Test
    void testSnapshotState() {
        long checkpointId = 100L;

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).commitTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        String transactionId = "txn-001";
        sender.beginTransaction(transactionId);
        List<KafkaSinkState> states = sender.snapshotState(checkpointId);

        Assertions.assertNotNull(states);
        Assertions.assertEquals(1, states.size());
        KafkaSinkState state = states.get(0);
        Assertions.assertEquals(transactionId, state.getTransactionId());
        Assertions.assertEquals(transactionPrefix, state.getTransactionIdPrefix());
        Assertions.assertEquals(checkpointId, state.getCheckpointId());
        Assertions.assertEquals(kafkaProperties, state.getKafkaProperties());
        verify(mockProducer, times(1)).commitTransaction();
    }

    @Test
    void testClose() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close(Mockito.any());
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.close();

        verify(mockProducer, times(1)).flush();
        verify(mockProducer, times(1)).close(Mockito.any());
    }

    @Test
    void testCloseWithNullProducer() {
        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        Assertions.assertDoesNotThrow(() -> sender.close());
    }

    @Test
    void testProducerReuse() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).setTransactionalId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");

        Assertions.assertEquals(1, mockedProducerConstruction.constructed().size());

        sender.beginTransaction("txn-002");

        Assertions.assertEquals(2, mockedProducerConstruction.constructed().size());
        verify(mockProducer, never()).setTransactionalId(anyString());
        verify(mockProducer, times(1)).initTransactions();
    }

    @Test
    void testMultipleTransactionsSameProducer() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).setTransactionalId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.beginTransaction("txn-002");
        sender.beginTransaction("txn-003");

        Assertions.assertEquals(3, mockedProducerConstruction.constructed().size());
        verify(mockProducer, never()).setTransactionalId(anyString());
        verify(mockProducer, times(1)).initTransactions();
    }

    @Test
    void testProducerNotClosedBetweenTransactions() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).setTransactionalId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).close(Mockito.any());
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.beginTransaction("txn-002");

        verify(mockProducer, times(0)).close(Mockito.any());
    }

    @Test
    void testAbortTransactionWithExistingProducer() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactionId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).flush();
                            when(mock.getEpoch()).thenReturn((short) 1).thenReturn((short) 0);
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.abortTransaction(100L);

        Assertions.assertEquals(1, mockedProducerConstruction.constructed().size());
    }

    @Test
    void testAbortTransactionWithoutExistingProducer() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactionId(anyString());
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).flush();
                            when(mock.getEpoch()).thenReturn((short) 1).thenReturn((short) 0);
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.abortTransaction(100L);

        Assertions.assertEquals(1, mockedProducerConstruction.constructed().size());
    }

    @Test
    void testTransactionalIdConfiguration() {
        String testTxnId = "txn-001";

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            Properties props = (Properties) context.arguments().get(0);
                            String txnId =
                                    props.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
                            Assertions.assertNotNull(txnId);
                            Assertions.assertEquals(testTxnId, txnId);
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction(testTxnId);
    }

    @Test
    void testPropertiesNotModified() {
        Properties originalProps = new Properties();
        originalProps.putAll(kafkaProperties);
        int originalSize = originalProps.size();

        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, originalProps);

        sender.beginTransaction("txn-001");

        Assertions.assertEquals(originalSize, originalProps.size());
        Assertions.assertNull(originalProps.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    void testCloseResetsProducerReference() {
        mockedProducerConstruction =
                Mockito.mockConstruction(
                        KafkaInternalProducer.class,
                        (mock, context) -> {
                            mockProducer = (KafkaInternalProducer<String, String>) mock;
                            doNothing().when(mock).initTransactions();
                            doNothing().when(mock).beginTransaction();
                            doNothing().when(mock).flush();
                            doNothing().when(mock).close(Mockito.any());
                        });

        KafkaTransactionSender<String, String> sender =
                new KafkaTransactionSender<>(transactionPrefix, kafkaProperties);

        sender.beginTransaction("txn-001");
        sender.close();

        sender.beginTransaction("txn-002");

        Assertions.assertEquals(2, mockedProducerConstruction.constructed().size());
    }
}
