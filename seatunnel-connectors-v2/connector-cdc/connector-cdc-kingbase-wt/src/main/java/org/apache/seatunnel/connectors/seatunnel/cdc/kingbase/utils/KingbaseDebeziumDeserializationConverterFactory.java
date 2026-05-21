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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializationConverters;
import org.apache.seatunnel.connectors.cdc.debezium.utils.TemporalConversions;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.kingbase8.geometric.KBpoint;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.HexConverter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

public class KingbaseDebeziumDeserializationConverterFactory
        implements DebeziumDeserializationConverterFactory {
    public static final DebeziumDeserializationConverterFactory INSTANCE =
            new KingbaseDebeziumDeserializationConverterFactory();

    @Override
    public Optional<DebeziumDeserializationConverter> createUserDefinedConverter(
            SeaTunnelDataType<?> type, ZoneId serverTimeZone) {
        switch (type.getSqlType()) {
            case STRING:
                return Optional.of(
                        new DebeziumDeserializationConverter() {

                            @Override
                            public Object convert(Object dbzObj, Schema schema) throws Exception {
                                if (dbzObj instanceof Struct) {
                                    Struct struct = (Struct) dbzObj;
                                    switch (schema.name()) {
                                        case Point.LOGICAL_NAME:
                                            return new KBpoint(
                                                            struct.getFloat64(Point.X_FIELD),
                                                            struct.getFloat64(Point.Y_FIELD))
                                                    .getValue();
                                        case Geometry.LOGICAL_NAME:
                                        case Geography.LOGICAL_NAME:
                                            return HexConverter.convertToHexString(
                                                    struct.getBytes(Geometry.WKB_FIELD));
                                        default:
                                            return dbzObj.toString();
                                    }
                                }
                                return dbzObj.toString();
                            }
                        });
            case TIMESTAMP:
                return Optional.of(
                        new DebeziumDeserializationConverter() {

                            @Override
                            public Object convert(Object dbzObj, Schema schema) throws Exception {
                                return convertTimestampValue(dbzObj, schema, serverTimeZone);
                            }
                        });
        }
        return Optional.empty();
    }

    /**
     * Kingbase uses the generic Debezium temporal logical types, but the shared converter treats
     * long-based timestamps as UTC wall-clock values and also expands microseconds incorrectly.
     * Keep the fix in the Kingbase adapter so we do not widen the blast radius to other CDC
     * connectors.
     */
    static Object convertTimestampValue(Object dbzObj, Schema schema, ZoneId serverTimeZone) {
        if (dbzObj == null) {
            return null;
        }
        if (dbzObj instanceof Long) {
            return convertLongTimestamp((Long) dbzObj, schema, serverTimeZone);
        }
        if (dbzObj instanceof String && ZonedTimestamp.SCHEMA_NAME.equals(schema.name())) {
            return ZonedDateTime.parse((String) dbzObj)
                    .withZoneSameInstant(serverTimeZone)
                    .toLocalDateTime();
        }
        // Local TIMESTAMP values can reach us as UTC-normalized strings/instants after Debezium's
        // JDBC value conversion. Preserve their wall-clock fields instead of shifting them again.
        if (dbzObj instanceof String) {
            return Instant.parse((String) dbzObj).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        if (dbzObj instanceof Instant) {
            return ((Instant) dbzObj).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        return TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
    }

    /**
     * Debezium long-based TIMESTAMP payloads represent local wall-clock fields, not absolute
     * instants. Keep the base/MySQL reconstruction model and only fix the microsecond remainder
     * math for MicroTimestamp.
     */
    private static Object convertLongTimestamp(long value, Schema schema, ZoneId serverTimeZone) {
        switch (schema.name()) {
            case Timestamp.SCHEMA_NAME:
                return SeaTunnelRowDebeziumDeserializationConverters.toLocalDateTime(value, 0);
            case MicroTimestamp.SCHEMA_NAME:
                return SeaTunnelRowDebeziumDeserializationConverters.toLocalDateTime(
                        value / 1_000L, (int) (value % 1_000L) * 1_000);
            case NanoTimestamp.SCHEMA_NAME:
                return SeaTunnelRowDebeziumDeserializationConverters.toLocalDateTime(
                        value / 1_000_000L, (int) (value % 1_000_000L));
            default:
                return value;
        }
    }
}
