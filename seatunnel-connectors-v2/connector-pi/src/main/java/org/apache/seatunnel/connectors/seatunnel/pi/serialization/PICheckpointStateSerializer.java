package org.apache.seatunnel.connectors.seatunnel.pi.serialization;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PICheckpointState;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PISplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * PI checkpoint state serializer
 *
 * <p>Used for checkpoint mechanism, serializing and deserializing PICheckpointState objects
 */
public class PICheckpointStateSerializer implements Serializer<PICheckpointState> {

    private static final int VERSION = 1;

    @Override
    public byte[] serialize(PICheckpointState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // Write version number
            oos.writeInt(VERSION);

            // Write checkpoint ID
            oos.writeLong(state.getCheckpointId());

            // Write pending splits
            Map<Integer, List<PISplit>> pendingSplits = state.getPendingSplits();
            oos.writeInt(pendingSplits.size());
            for (Map.Entry<Integer, List<PISplit>> entry : pendingSplits.entrySet()) {
                oos.writeInt(entry.getKey());
                List<PISplit> splits = entry.getValue();
                oos.writeInt(splits.size());
                for (PISplit split : splits) {
                    oos.writeUTF(split.getSplitId());
                    oos.writeInt(split.getPiPaths().size());
                    for (String webId : split.getPiPaths()) {
                        oos.writeUTF(webId);
                    }
                    oos.writeLong(split.getLastCheckpointTime());
                }
            }

            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PICheckpointState deserialize(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bais)) {

            // Read version number
            int version = ois.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported serialization version: " + version);
            }

            PICheckpointState state = new PICheckpointState();

            // Read checkpoint ID
            state.setCheckpointId(ois.readLong());

            // Read pending splits
            int pendingSplitsSize = ois.readInt();
            Map<Integer, List<PISplit>> pendingSplits = new java.util.HashMap<>();
            for (int i = 0; i < pendingSplitsSize; i++) {
                int readerId = ois.readInt();
                int splitsSize = ois.readInt();
                List<PISplit> splits = new java.util.ArrayList<>();
                for (int j = 0; j < splitsSize; j++) {
                    String splitId = ois.readUTF();
                    int webIdsSize = ois.readInt();
                    List<String> webIds = new java.util.ArrayList<>();
                    for (int k = 0; k < webIdsSize; k++) {
                        webIds.add(ois.readUTF());
                    }
                    long lastCheckpointTime = ois.readLong();
                    splits.add(new PISplit(splitId, webIds, lastCheckpointTime));
                }
                pendingSplits.put(readerId, splits);
            }
            state.setPendingSplits(pendingSplits);

            return state;
        }
    }
}
