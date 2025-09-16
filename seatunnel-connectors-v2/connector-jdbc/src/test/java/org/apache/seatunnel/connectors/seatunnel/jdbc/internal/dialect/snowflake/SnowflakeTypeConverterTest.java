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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for SnowflakeTypeConverter to verify timestamp type consistency and proper type mapping
 * after fixes.
 */
public class SnowflakeTypeConverterTest {

    private final SnowflakeTypeConverter converter = new SnowflakeTypeConverter();

    @Test
    public void testIdentifier() {
        assertEquals(DatabaseIdentifier.SNOWFLAKE, converter.identifier());
    }

    @Test
    public void testTimestampTypesConsistency() {
        // Test TIMESTAMP_LTZ with correct underscores
        BasicTypeDefine timestampLtzDefine =
                BasicTypeDefine.builder()
                        .name("test_ltz")
                        .columnType("TIMESTAMP_LTZ")
                        .dataType("TIMESTAMP_LTZ")
                        .build();

        Column ltzColumn = converter.convert(timestampLtzDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, ltzColumn.getDataType());
        assertEquals(9, ltzColumn.getScale());

        // Test TIMESTAMP_NTZ with correct underscores
        BasicTypeDefine timestampNtzDefine =
                BasicTypeDefine.builder()
                        .name("test_ntz")
                        .columnType("TIMESTAMP_NTZ")
                        .dataType("TIMESTAMP_NTZ")
                        .build();

        Column ntzColumn = converter.convert(timestampNtzDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, ntzColumn.getDataType());
        assertEquals(9, ntzColumn.getScale());

        // Test TIMESTAMP_TZ with correct underscores
        BasicTypeDefine timestampTzDefine =
                BasicTypeDefine.builder()
                        .name("test_tz")
                        .columnType("TIMESTAMP_TZ")
                        .dataType("TIMESTAMP_TZ")
                        .build();

        Column tzColumn = converter.convert(timestampTzDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, tzColumn.getDataType());
        assertEquals(9, tzColumn.getScale());
    }

    @Test
    public void testStringTypeLengthHandling() {
        // Test small string
        PhysicalColumn smallStringColumn =
                PhysicalColumn.builder()
                        .name("small_string")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(100L)
                        .build();

        BasicTypeDefine smallResult = converter.reconvert(smallStringColumn);
        assertEquals("VARCHAR(100)", smallResult.getColumnType());
        assertEquals("VARCHAR", smallResult.getDataType());

        // Test large string - should use TEXT instead of BINARY after fix
        PhysicalColumn largeStringColumn =
                PhysicalColumn.builder()
                        .name("large_string")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(20000000L)
                        .build();

        BasicTypeDefine largeResult = converter.reconvert(largeStringColumn);
        assertEquals("TEXT", largeResult.getColumnType());
        assertEquals("TEXT", largeResult.getDataType());
    }

    @Test
    public void testGeographyAndGeometryTypes() {
        // Test GEOGRAPHY type
        BasicTypeDefine geographyDefine =
                BasicTypeDefine.builder()
                        .name("geo_col")
                        .columnType("GEOGRAPHY")
                        .dataType("GEOGRAPHY")
                        .build();

        Column geoColumn = converter.convert(geographyDefine);
        assertEquals(BasicType.STRING_TYPE, geoColumn.getDataType());

        // Test GEOMETRY type
        BasicTypeDefine geometryDefine =
                BasicTypeDefine.builder()
                        .name("geom_col")
                        .columnType("GEOMETRY")
                        .dataType("GEOMETRY")
                        .build();

        Column geomColumn = converter.convert(geometryDefine);
        assertEquals(BasicType.STRING_TYPE, geomColumn.getDataType());
    }

    @Test
    public void testDecimalTypeHandling() {
        // Test normal decimal
        BasicTypeDefine decimalDefine =
                BasicTypeDefine.builder()
                        .name("decimal_col")
                        .columnType("DECIMAL(10,2)")
                        .dataType("DECIMAL")
                        .precision(10L)
                        .scale(2)
                        .build();

        Column decimalColumn = converter.convert(decimalDefine);
        assertTrue(decimalColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) decimalColumn.getDataType();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testBinaryTypeHandling() {
        // Test BINARY type
        BasicTypeDefine binaryDefine =
                BasicTypeDefine.builder()
                        .name("binary_col")
                        .columnType("BINARY")
                        .dataType("BINARY")
                        .build();

        Column binaryColumn = converter.convert(binaryDefine);
        assertEquals(PrimitiveByteArrayType.INSTANCE, binaryColumn.getDataType());
    }
}
