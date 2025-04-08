package org.apache.seatunnel.connectors.argodb.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@AllArgsConstructor
@Slf4j
public class ArgoDBResourceManager implements MultiTableResourceManager<ArgoDBClient> {

    private ArgoDBClient client;

    @Override
    public Optional<ArgoDBClient> getSharedResource() {
        return Optional.of(client);
    }

    @Override
    public void close() {
        log.info("Closing ArgoDBClient ...");
        client.close();
    }
}
