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

package io.debezium.connector.kingbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class KingbaseValueConverterTest {

    @Test
    public void testSnapshotTimestampStringWithTimezoneParsesToLocalDateTime() {
        TestableKingbaseValueConverter converter = new TestableKingbaseValueConverter();

        Object converted = converter.normalizeTimestampValueForTest("2026-05-06T12:10:01.111+0800");

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 12, 10, 1, 111_000_000), converted);
    }

    @Test
    public void testSnapshotTimestampStringWithoutTimezoneParsesToLocalDateTime() {
        TestableKingbaseValueConverter converter = new TestableKingbaseValueConverter();

        Object converted = converter.normalizeTimestampValueForTest("2026-05-06 12:10:02.222222");

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 12, 10, 2, 222_222_000), converted);
    }

    @Test
    public void testSnapshotTimestampPreservesWallClockValue() {
        TestableKingbaseValueConverter converter = new TestableKingbaseValueConverter();

        Object converted =
                converter.normalizeTimestampValueForTest(
                        Timestamp.valueOf(LocalDateTime.of(2026, 5, 6, 13, 30, 1, 111_111_000)));

        Assertions.assertEquals(LocalDateTime.of(2026, 5, 6, 13, 30, 1, 111_111_000), converted);
    }

    private static final class TestableKingbaseValueConverter extends KingbaseValueConverter {

        private TestableKingbaseValueConverter() {
            super(
                    StandardCharsets.UTF_8,
                    DecimalMode.PRECISE,
                    TemporalPrecisionMode.ADAPTIVE,
                    ZoneOffset.UTC,
                    null,
                    false,
                    null,
                    KingbaseConnectorConfig.HStoreHandlingMode.JSON,
                    BinaryHandlingMode.BYTES,
                    KingbaseConnectorConfig.IntervalHandlingMode.NUMERIC,
                    new byte[0],
                    2);
        }

        private Object normalizeTimestampValueForTest(Object data) {
            return convertTimestampToLocalDateTime(null, null, data);
        }
    }
}
