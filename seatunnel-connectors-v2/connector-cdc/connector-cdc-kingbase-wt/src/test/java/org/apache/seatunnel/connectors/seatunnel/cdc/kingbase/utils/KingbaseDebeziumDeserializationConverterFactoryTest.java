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

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class KingbaseDebeziumDeserializationConverterFactoryTest {

    @Test
    public void testConvertMicroTimestampRestoresSourceTimezoneAndMicros() throws Exception {
        DebeziumDeserializationConverter converter =
                createTimestampConverter(ZoneId.of("Asia/Shanghai"));
        Schema schema = SchemaBuilder.int64().name(MicroTimestamp.SCHEMA_NAME).build();
        long epochMicros = 1778074201111111L;

        Object converted = converter.convert(epochMicros, schema);

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 13, 30, 1, 111_111_000), converted);
    }

    @Test
    public void testConvertMillisecondTimestampRestoresSourceTimezone() throws Exception {
        DebeziumDeserializationConverter converter =
                createTimestampConverter(ZoneId.of("Asia/Shanghai"));
        Schema schema = SchemaBuilder.int64().name(Timestamp.SCHEMA_NAME).build();
        long epochMillis = Instant.parse("2026-05-06T13:30:01.111Z").toEpochMilli();

        Object converted = converter.convert(epochMillis, schema);

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 13, 30, 1, 111_000_000), converted);
    }

    @Test
    public void testConvertInstantTimestampKeepsWallClockTime() throws Exception {
        DebeziumDeserializationConverter converter =
                createTimestampConverter(ZoneId.of("Asia/Shanghai"));
        Schema schema = SchemaBuilder.string().name(Timestamp.SCHEMA_NAME).build();
        Instant instant = Instant.parse("2026-05-06T13:30:01.111Z");

        Object converted = converter.convert(instant, schema);

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 13, 30, 1, 111_000_000), converted);
    }

    /** Creates the same TIMESTAMP converter that SeaTunnel wires into the Kingbase CDC runtime. */
    private DebeziumDeserializationConverter createTimestampConverter(ZoneId serverTimeZone) {
        Optional<DebeziumDeserializationConverter> converter =
                KingbaseDebeziumDeserializationConverterFactory.INSTANCE.createUserDefinedConverter(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE, serverTimeZone);
        Assertions.assertTrue(converter.isPresent());
        return converter.get();
    }
}
