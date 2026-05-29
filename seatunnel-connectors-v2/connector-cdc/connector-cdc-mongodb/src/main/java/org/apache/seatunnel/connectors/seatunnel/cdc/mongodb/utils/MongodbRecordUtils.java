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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.bson.json.JsonWriterSettings;

import com.mongodb.kafka.connect.source.json.formatter.DefaultJson;
import com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults;
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue;
import io.debezium.relational.TableId;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.mongodb.kafka.connect.source.schema.AvroSchema.fromJson;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COPY_KEY_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.HEARTBEAT_KEY_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OUTPUT_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;

public class MongodbRecordUtils {
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /** Check the sourceRecord is snapshot record. */
    public static boolean isSnapshotRecord(SourceRecord sourceRecord) {
        return "true".equals(getOffsetValue(sourceRecord, COPY_KEY_FIELD));
    }

    /** Check the sourceRecord is heartbeat event. */
    public static boolean isHeartbeatEvent(SourceRecord sourceRecord) {
        return "true".equals(getOffsetValue(sourceRecord, HEARTBEAT_KEY_FIELD));
    }

    public static boolean isDataChangeRecord(SourceRecord sourceRecord) {
        return !isWatermarkEvent(sourceRecord) && !isHeartbeatEvent(sourceRecord);
    }

    public static BsonDocument getResumeToken(SourceRecord sourceRecord) {
        return BsonDocument.parse(getOffsetValue(sourceRecord, ID_FIELD));
    }

    public static BsonDocument getDocumentKey(@Nonnull SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        return extractBsonDocument(value, sourceRecord.valueSchema(), DOCUMENT_KEY);
    }

    public static BsonDocument extractBsonDocument(
            Struct value, @Nonnull Schema valueSchema, String fieldName) {
        if (valueSchema.field(fieldName) != null) {
            String docString = value.getString(fieldName);
            if (docString != null) {
                return parseBsonDocument(docString);
            }
        }
        return null;
    }

    private static BsonDocument parseBsonDocument(String docString) {
        try {
            return BsonDocument.parse(docString);
        } catch (JsonParseException e) {
            String normalizedDocString = normalizeScientificDate(docString);
            if (docString.equals(normalizedDocString)) {
                throw e;
            }
            try {
                return BsonDocument.parse(normalizedDocString);
            } catch (JsonParseException normalizedParseException) {
                normalizedParseException.addSuppressed(e);
                throw normalizedParseException;
            }
        }
    }

    private static String normalizeScientificDate(String docString) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(docString);
            if (!normalizeScientificDate(root)) {
                return docString;
            }
            return OBJECT_MAPPER.writeValueAsString(root);
        } catch (IOException e) {
            return docString;
        }
    }

    private static boolean normalizeScientificDate(JsonNode node) {
        if (node == null) {
            return false;
        }
        if (node.isArray()) {
            boolean changed = false;
            for (JsonNode child : (ArrayNode) node) {
                changed |= normalizeScientificDate(child);
            }
            return changed;
        }
        if (!node.isObject()) {
            return false;
        }

        ObjectNode objectNode = (ObjectNode) node;
        JsonNode dateNode = objectNode.get("$date");
        if (objectNode.size() == 1 && dateNode != null && dateNode.isNumber()) {
            ObjectNode numberLongNode = OBJECT_MAPPER.createObjectNode();
            numberLongNode.put("$numberLong", dateNode.decimalValue().toBigInteger().toString());
            objectNode.set("$date", numberLongNode);
            return true;
        }

        boolean changed = false;
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            changed |= normalizeScientificDate(fields.next().getValue());
        }
        return changed;
    }

    public static String getOffsetValue(@Nonnull SourceRecord sourceRecord, String key) {
        return (String) sourceRecord.sourceOffset().get(key);
    }

    public static @Nonnull TableId getTableId(@Nonnull SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(NS_FIELD);
        String dbName = source.getString(DB_FIELD);
        String collName = source.getString(COLL_FIELD);
        return new TableId(dbName, null, collName);
    }

    public static @Nonnull BsonTimestamp currentBsonTimestamp() {
        return bsonTimestampFromEpochMillis(System.currentTimeMillis());
    }

    public static @Nonnull BsonTimestamp maximumBsonTimestamp() {
        return new BsonTimestamp(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static @Nonnull BsonTimestamp bsonTimestampFromEpochMillis(long epochMillis) {
        return new BsonTimestamp((int) Instant.ofEpochMilli(epochMillis).getEpochSecond(), 1);
    }

    public static @Nonnull SourceRecord buildSourceRecord(
            final Map<String, String> partition,
            final Map<String, String> sourceOffset,
            final String topicName,
            final BsonDocument keyDocument,
            final BsonDocument valueDocument) {
        return buildSourceRecord(
                partition,
                sourceOffset,
                topicName,
                keyDocument,
                valueDocument,
                new DefaultJson().getJsonWriterSettings());
    }

    public static @Nonnull SourceRecord buildSourceRecord(
            Map<String, String> partition,
            Map<String, String> sourceOffset,
            String topicName,
            BsonDocument keyDocument,
            BsonDocument valueDocument,
            JsonWriterSettings jsonWriterSettings) {
        BsonValueToSchemaAndValue schemaAndValue =
                new BsonValueToSchemaAndValue(jsonWriterSettings);
        SchemaAndValue keySchemaAndValue =
                schemaAndValue.toSchemaAndValue(
                        fromJson(AvroSchemaDefaults.DEFAULT_AVRO_KEY_SCHEMA), keyDocument);
        BsonDocument source = valueDocument.get(SOURCE_FIELD).asDocument();
        BsonValue table = valueDocument.get(NS_FIELD).asDocument().get(COLL_FIELD);
        BsonValue db = valueDocument.get(NS_FIELD).asDocument().get(DB_FIELD);
        source.append(TABLE_NAME_KEY, table);
        source.append(DB_FIELD, db);
        valueDocument.replace(SOURCE_FIELD, source);
        SchemaAndValue valueSchemaAndValue =
                schemaAndValue.toSchemaAndValue(fromJson(OUTPUT_SCHEMA), valueDocument);

        return new SourceRecord(
                partition,
                sourceOffset,
                topicName,
                keySchemaAndValue.schema(),
                keySchemaAndValue.value(),
                valueSchemaAndValue.schema(),
                valueSchemaAndValue.value());
    }

    public static @Nonnull SourceRecord buildSourceRecord(
            Map<String, ?> sourcePartition,
            Map<String, ?> sourceOffset,
            String topicName,
            Integer partition,
            Schema keySchema,
            Object key,
            BsonDocument valueDocument) {
        BsonValueToSchemaAndValue schemaAndValue =
                new BsonValueToSchemaAndValue(new DefaultJson().getJsonWriterSettings());
        SchemaAndValue valueSchemaAndValue =
                schemaAndValue.toSchemaAndValue(fromJson(OUTPUT_SCHEMA), valueDocument);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topicName,
                partition,
                keySchema,
                key,
                valueSchemaAndValue.schema(),
                valueSchemaAndValue.value());
    }

    public static @Nonnull Map<String, String> createSourceOffsetMap(
            @Nonnull BsonDocument idDocument, boolean isSnapshotRecord) {
        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, idDocument.toJson());
        sourceOffset.put(COPY_KEY_FIELD, String.valueOf(isSnapshotRecord));
        return sourceOffset;
    }

    public static @Nonnull Map<String, String> createPartitionMap(
            String hosts, String database, String collection) {
        StringBuilder builder = new StringBuilder();
        builder.append("mongodb://");
        builder.append(hosts);
        builder.append("/");
        if (StringUtils.isNotEmpty(database)) {
            builder.append(database);
        }
        if (StringUtils.isNotEmpty(collection)) {
            builder.append(".");
            builder.append(collection);
        }
        return Collections.singletonMap(NS_FIELD, builder.toString());
    }

    public static @Nonnull Map<String, Object> createHeartbeatPartitionMap(String hosts) {
        String builder = "mongodb://" + hosts + "/" + "__mongodb_heartbeats";
        return Collections.singletonMap(NS_FIELD, builder);
    }

    public static @Nonnull Map<String, String> createWatermarkPartitionMap(String partition) {
        return Collections.singletonMap(NS_FIELD, partition);
    }
}
