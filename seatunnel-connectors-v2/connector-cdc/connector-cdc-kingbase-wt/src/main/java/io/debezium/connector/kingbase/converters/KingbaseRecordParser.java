/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

import java.util.Set;

/**
 * Parser for records produced by Kingbase connectors.
 *
 * @author Chris Cranford
 */
public class KingbaseRecordParser extends RecordParser {

    static final String TXID_KEY = "txId";
    static final String XMIN_KEY = "xmin";
    static final String LSN_KEY = "lsn";
    static final Set<String> SOURCE_FIELDS =
            Collect.unmodifiableSet("version", "connector", "name", "ts_ms", "snapshot", "db");

    static final Set<String> POSTGRES_SOURCE_FIELD =
            Collect.unmodifiableSet(TXID_KEY, XMIN_KEY, LSN_KEY);

    public KingbaseRecordParser(Schema schema, Struct record) {
        super(schema, record, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (POSTGRES_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException(
                "No such field \""
                        + name
                        + "\" in the \"source\" field of events from Kingbase connector");
    }
}
