/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import io.debezium.connector.kingbase.utils.Partition;
import io.debezium.util.Collect;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KingbasePartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public KingbasePartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KingbasePartition other = (KingbasePartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    static class Provider implements Partition.Provider<KingbasePartition> {
        private final KingbaseConnectorConfig connectorConfig;

        Provider(KingbaseConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<KingbasePartition> getPartitions() {
            return Collections.singleton(new KingbasePartition(connectorConfig.getLogicalName()));
        }
    }
}
