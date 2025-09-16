package org.apache.seatunnel.connectors.seatunnel.pi.serialization;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.connectors.seatunnel.pi.split.PISplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * PI split serializer
 *
 * <p>Used for checkpoint mechanism, serializing and deserializing PISplit objects
 */
public class PISplitSerializer implements Serializer<PISplit> {

    private static final int VERSION = 1;

    @Override
    public byte[] serialize(PISplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // Write version number
            oos.writeInt(VERSION);

            // Write split ID
            oos.writeUTF(split.getSplitId());

            // Write piPaths list
            List<String> piPaths = split.getPiPaths();
            oos.writeInt(piPaths.size());
            for (String piPath : piPaths) {
                oos.writeUTF(piPath);
            }

            // Write checkpoint time
            oos.writeLong(split.getLastCheckpointTime());

            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PISplit deserialize(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bais)) {

            // Read version number
            int version = ois.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported serialization version: " + version);
            }

            // Read split ID
            String splitId = ois.readUTF();

            // Read piPaths list
            int piPathsCount = ois.readInt();
            List<String> webIds = new ArrayList<>(piPathsCount);
            for (int i = 0; i < piPathsCount; i++) {
                webIds.add(ois.readUTF());
            }

            // Read checkpoint time
            long lastCheckpointTime = ois.readLong();

            return new PISplit(splitId, webIds, lastCheckpointTime);
        }
    }
}
