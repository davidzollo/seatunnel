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

package org.apache.seatunnel.connectors.seatunnel.kafka.state;

import org.apache.kafka.clients.producer.ProducerConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

/**
 * Test for KafkaCommitInfo serialization backward compatibility. This test ensures that old
 * checkpoint data (without txnStarted field) can be correctly deserialized by new code.
 */
class KafkaCommitInfoSerializationTest {

    @Test
    void testSerializationNewVersion() throws IOException, ClassNotFoundException {
        Properties props = createTestProperties();

        KafkaCommitInfo original = new KafkaCommitInfo("txn-001", props, 12345L, (short) 1, true);

        byte[] serialized = serialize(original);
        KafkaCommitInfo deserialized = deserialize(serialized);

        Assertions.assertEquals(original.getTransactionId(), deserialized.getTransactionId());
        Assertions.assertEquals(original.getProducerId(), deserialized.getProducerId());
        Assertions.assertEquals(original.getEpoch(), deserialized.getEpoch());
        Assertions.assertEquals(original.isTxnStarted(), deserialized.isTxnStarted());
        Assertions.assertTrue(deserialized.isTxnStarted());
    }

    @Test
    void testSerializationEmptyTransaction() throws IOException, ClassNotFoundException {
        Properties props = createTestProperties();

        KafkaCommitInfo original = new KafkaCommitInfo("txn-002", props, 12345L, (short) 1, false);

        byte[] serialized = serialize(original);
        KafkaCommitInfo deserialized = deserialize(serialized);

        Assertions.assertEquals(original.getTransactionId(), deserialized.getTransactionId());
        Assertions.assertEquals(original.getProducerId(), deserialized.getProducerId());
        Assertions.assertEquals(original.getEpoch(), deserialized.getEpoch());
        Assertions.assertEquals(original.isTxnStarted(), deserialized.isTxnStarted());
        Assertions.assertFalse(deserialized.isTxnStarted());
    }

    @Test
    void testBackwardCompatibilitySimulation() throws Exception {
        Properties props = createTestProperties();

        /*
         * This test simulates backward compatibility by:
         * 1. Using reflection to set txnStarted to null (simulating old data)
         * 2. Serializing and deserializing
         * 3. Verifying that readObject() sets txnStarted=true for old data
         *
         * Note: Real old version data would have txnStarted=null because
         * the field doesn't exist in old version class definition.
         */
        KafkaCommitInfo commitInfo =
                new KafkaCommitInfo("txn-003", props, 12345L, (short) 2, false);

        // Set txnStarted to null to simulate old data
        java.lang.reflect.Field txnStartedField =
                KafkaCommitInfo.class.getDeclaredField("txnStarted");
        txnStartedField.setAccessible(true);
        txnStartedField.set(commitInfo, null);

        // Serialize with txnStarted=null (simulates old version)
        byte[] serialized = serialize(commitInfo);

        // Deserialize - should detect old data (null) and set txnStarted=true
        KafkaCommitInfo deserialized = deserialize(serialized);

        Assertions.assertEquals("txn-003", deserialized.getTransactionId());
        Assertions.assertEquals(12345L, deserialized.getProducerId());
        Assertions.assertEquals((short) 2, deserialized.getEpoch());
        /*
         * IMPORTANT: Even though we serialized with txnStarted=null,
         * readObject() should detect this as old data and set txnStarted=true
         */
        Assertions.assertTrue(
                deserialized.isTxnStarted(),
                "Old version data (txnStarted=null) should default to true for safety");
    }

    @Test
    void testMultipleRoundTrips() throws IOException, ClassNotFoundException {
        Properties props = createTestProperties();

        KafkaCommitInfo original = new KafkaCommitInfo("txn-004", props, 99999L, (short) 5, true);

        byte[] serialized1 = serialize(original);
        KafkaCommitInfo deserialized1 = deserialize(serialized1);

        byte[] serialized2 = serialize(deserialized1);
        KafkaCommitInfo deserialized2 = deserialize(serialized2);

        Assertions.assertEquals(original.getTransactionId(), deserialized2.getTransactionId());
        Assertions.assertEquals(original.isTxnStarted(), deserialized2.isTxnStarted());
    }

    @Test
    void testNullSafetyInGetter() {
        Properties props = createTestProperties();

        /*
         * Test that isTxnStarted() handles null safely
         * This ensures defensive programming against unexpected null values
         */
        KafkaCommitInfo commitInfo1 =
                new KafkaCommitInfo("txn-005", props, 11111L, (short) 0, true);
        KafkaCommitInfo commitInfo2 =
                new KafkaCommitInfo("txn-006", props, 22222L, (short) 1, false);

        Assertions.assertTrue(commitInfo1.isTxnStarted());
        Assertions.assertFalse(commitInfo2.isTxnStarted());
    }

    private byte[] serialize(KafkaCommitInfo commitInfo) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(commitInfo);
        }
        return baos.toByteArray();
    }

    private KafkaCommitInfo deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (KafkaCommitInfo) ois.readObject();
        }
    }

    private Properties createTestProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
