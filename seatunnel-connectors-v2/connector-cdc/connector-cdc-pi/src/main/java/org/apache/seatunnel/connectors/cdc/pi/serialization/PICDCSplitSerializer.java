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
import org.apache.seatunnel.connectors.cdc.pi.split.PICDCSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * PI CDC split serializer
 *
 * <p>Used for checkpoint mechanism, serialize and deserialize PICDCSplit object
 */
public class PICDCSplitSerializer implements Serializer<PICDCSplit> {

    private static final int VERSION = 1;

    @Override
    public byte[] serialize(PICDCSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // Write version number
            oos.writeInt(VERSION);

            // Write split ID
            oos.writeUTF(split.splitId());

            // Write PI Path list
            List<String> piPaths = split.getPiPaths();
            oos.writeInt(piPaths.size());
            for (String piPath : piPaths) {
                oos.writeUTF(piPath);
            }

            // Skip WebID list - only use PI Paths

            // Write checkpoint time
            oos.writeLong(split.getLastCheckpointTime());

            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PICDCSplit deserialize(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bais)) {

            // Read version number
            int version = ois.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported serialization version: " + version);
            }

            // Read split ID
            String splitId = ois.readUTF();

            // Read PI Path list
            int piPathCount = ois.readInt();
            List<String> piPaths = new ArrayList<>(piPathCount);
            for (int i = 0; i < piPathCount; i++) {
                piPaths.add(ois.readUTF());
            }

            // Read checkpoint time
            long lastCheckpointTime = ois.readLong();

            return new PICDCSplit(splitId, piPaths, lastCheckpointTime);
        }
    }
}
