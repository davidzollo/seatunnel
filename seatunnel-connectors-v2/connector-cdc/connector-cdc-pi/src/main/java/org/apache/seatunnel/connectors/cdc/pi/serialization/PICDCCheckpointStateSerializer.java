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

package org.apache.seatunnel.connectors.cdc.pi.serialization;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCCheckpointState;
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * PI CDC checkpoint state serializer
 *
 * <p>Used for SourceSplitEnumerator checkpoint mechanism, serialize and deserialize
 * PICDCCheckpointState object
 */
public class PICDCCheckpointStateSerializer implements Serializer<PICDCCheckpointState> {

    private static final int VERSION = 1;

    @Override
    public byte[] serialize(PICDCCheckpointState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // Write version number
            oos.writeInt(VERSION);

            // Write remaining splits
            List<PICDCSplit> remainingSplits = state.getRemainingSplits();
            oos.writeInt(remainingSplits.size());
            for (PICDCSplit split : remainingSplits) {
                oos.writeObject(split);
            }

            // Write assigned splits
            List<PICDCSplit> assignedSplits = state.getAssignedSplits();
            oos.writeInt(assignedSplits.size());
            for (PICDCSplit split : assignedSplits) {
                oos.writeObject(split);
            }

            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PICDCCheckpointState deserialize(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bais)) {

            // Read version number
            int version = ois.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported serialization version: " + version);
            }

            // Read remaining splits
            int remainingSplitsSize = ois.readInt();
            List<PICDCSplit> remainingSplits = new ArrayList<>(remainingSplitsSize);
            for (int i = 0; i < remainingSplitsSize; i++) {
                try {
                    PICDCSplit split = (PICDCSplit) ois.readObject();
                    remainingSplits.add(split);
                } catch (ClassNotFoundException e) {
                    throw new IOException("Failed to deserialize PICDCSplit object", e);
                }
            }

            // Read assigned splits
            int assignedSplitsSize = ois.readInt();
            List<PICDCSplit> assignedSplits = new ArrayList<>(assignedSplitsSize);
            for (int i = 0; i < assignedSplitsSize; i++) {
                try {
                    PICDCSplit split = (PICDCSplit) ois.readObject();
                    assignedSplits.add(split);
                } catch (ClassNotFoundException e) {
                    throw new IOException("Failed to deserialize PICDCSplit object", e);
                }
            }

            return new PICDCCheckpointState(remainingSplits, assignedSplits);
        }
    }
}
