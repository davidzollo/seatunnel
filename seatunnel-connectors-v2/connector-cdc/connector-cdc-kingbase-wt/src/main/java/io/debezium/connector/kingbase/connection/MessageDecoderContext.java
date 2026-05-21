/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.connection;

import io.debezium.connector.kingbase.KingbaseConnectorConfig;
import io.debezium.connector.kingbase.KingbaseSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final KingbaseConnectorConfig config;
    private final KingbaseSchema schema;

    public MessageDecoderContext(KingbaseConnectorConfig config, KingbaseSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public KingbaseConnectorConfig getConfig() {
        return config;
    }

    public KingbaseSchema getSchema() {
        return schema;
    }
}
