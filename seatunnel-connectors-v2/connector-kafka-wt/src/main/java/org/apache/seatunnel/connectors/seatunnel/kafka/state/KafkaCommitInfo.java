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

import lombok.Data;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Properties;

@Data
public class KafkaCommitInfo implements Serializable {

    private static final long serialVersionUID = -3468384927842600554L;

    private final String transactionId;
    private final Properties kafkaProperties;
    private final long producerId;
    private final short epoch;

    /*
     * IMPORTANT: This field was added for empty transaction handling.
     * For backward compatibility with old checkpoints (which don't have this field):
     * - Using Boolean (wrapper class) instead of boolean (primitive)
     * - Old data: field doesn't exist, deserializes to null
     * - New data: explicitly set to true or false
     * - readObject() detects null and sets safe default (true)
     *
     * Rationale for Boolean vs boolean:
     * - null value distinguishes old data (no field) from new data with false value
     * - No need for additional version marker field
     */
    private Boolean txnStarted;

    public KafkaCommitInfo(
            String transactionId,
            Properties kafkaProperties,
            long producerId,
            short epoch,
            boolean txnStarted) {
        this.transactionId = transactionId;
        this.kafkaProperties = kafkaProperties;
        this.producerId = producerId;
        this.epoch = epoch;
        this.txnStarted = txnStarted;
    }

    public boolean isTxnStarted() {
        // Defensive: treat null as true (should not happen after readObject)
        return txnStarted != null ? txnStarted : true;
    }

    /*
     * Custom deserialization to handle backward compatibility.
     * When deserializing old checkpoint data (without txnStarted field):
     * 1. txnStarted will be null (field doesn't exist in old data)
     * 2. We detect old data by checking if txnStarted is null
     * 3. For old data, set txnStarted to true (safe default: assume normal transaction)
     *
     * Rationale for defaulting to 'true':
     * - Old version didn't handle empty transactions
     * - All old CommitInfo instances represent real transactions that need committing
     * - Setting to false would cause Committer to skip them (data loss risk)
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (txnStarted == null) {
            // This is old version data (before txnStarted field was added)
            // Default to true: assume it's a normal transaction that needs committing
            txnStarted = Boolean.TRUE;
        }
    }
}
