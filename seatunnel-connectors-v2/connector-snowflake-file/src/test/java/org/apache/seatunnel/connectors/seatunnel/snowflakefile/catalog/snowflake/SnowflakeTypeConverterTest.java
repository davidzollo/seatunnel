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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog.snowflake;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog.SnowflakeTypeConverter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnowflakeTypeConverterTest {

    @Test
    public void testBasicTypeConversion() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;

        // Test STRING type
        BasicTypeDefine stringDefine =
                BasicTypeDefine.builder()
                        .name("test_string")
                        .columnType("VARCHAR(255)")
                        .dataType("VARCHAR")
                        .length(255L)
                        .nullable(true)
                        .build();

        Column stringColumn = converter.convert(stringDefine);
        assertNotNull(stringColumn);
        assertEquals("test_string", stringColumn.getName());
        assertEquals(BasicType.STRING_TYPE, stringColumn.getDataType());
        assertEquals("VARCHAR(255)", stringColumn.getSourceType());
    }

    @Test
    public void testNumericTypeConversion() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;

        // Test INTEGER type
        BasicTypeDefine intDefine =
                BasicTypeDefine.builder()
                        .name("test_int")
                        .columnType("INTEGER")
                        .dataType("INTEGER")
                        .nullable(true)
                        .build();

        Column intColumn = converter.convert(intDefine);
        assertNotNull(intColumn);
        assertEquals("test_int", intColumn.getName());
        assertEquals(BasicType.INT_TYPE, intColumn.getDataType());

        // Test BIGINT type
        BasicTypeDefine bigintDefine =
                BasicTypeDefine.builder()
                        .name("test_bigint")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .nullable(true)
                        .build();

        Column bigintColumn = converter.convert(bigintDefine);
        assertNotNull(bigintColumn);
        assertEquals("test_bigint", bigintColumn.getName());
        assertEquals(BasicType.LONG_TYPE, bigintColumn.getDataType());
    }

    @Test
    public void testDecimalTypeConversion() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;

        // Test DECIMAL type
        BasicTypeDefine decimalDefine =
                BasicTypeDefine.builder()
                        .name("test_decimal")
                        .columnType("DECIMAL(10,2)")
                        .dataType("DECIMAL")
                        .precision(10L)
                        .scale(2)
                        .nullable(true)
                        .build();

        Column decimalColumn = converter.convert(decimalDefine);
        assertNotNull(decimalColumn);
        assertEquals("test_decimal", decimalColumn.getName());
        assertTrue(decimalColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) decimalColumn.getDataType();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testDateTimeTypeConversion() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;

        // Test DATE type
        BasicTypeDefine dateDefine =
                BasicTypeDefine.builder()
                        .name("test_date")
                        .columnType("DATE")
                        .dataType("DATE")
                        .nullable(true)
                        .build();

        Column dateColumn = converter.convert(dateDefine);
        assertNotNull(dateColumn);
        assertEquals("test_date", dateColumn.getName());
        assertEquals(LocalTimeType.LOCAL_DATE_TYPE, dateColumn.getDataType());

        // Test TIMESTAMP type
        BasicTypeDefine timestampDefine =
                BasicTypeDefine.builder()
                        .name("test_timestamp")
                        .columnType("TIMESTAMP_NTZ")
                        .dataType("TIMESTAMP_NTZ")
                        .nullable(true)
                        .build();

        Column timestampColumn = converter.convert(timestampDefine);
        assertNotNull(timestampColumn);
        assertEquals("test_timestamp", timestampColumn.getName());
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampColumn.getDataType());
    }

    @Test
    public void testReconvertColumn() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;

        // Test reconvert STRING column
        Column stringColumn =
                PhysicalColumn.of(
                        "test_string",
                        BasicType.STRING_TYPE,
                        255L,
                        null,
                        true,
                        null,
                        "string comment");

        BasicTypeDefine stringDefine = converter.reconvert(stringColumn);
        assertNotNull(stringDefine);
        assertEquals("test_string", stringDefine.getName());
        assertEquals("VARCHAR(255)", stringDefine.getColumnType());
        assertEquals("VARCHAR", stringDefine.getDataType());
        assertEquals(255L, stringDefine.getLength());

        // Test reconvert INTEGER column
        Column intColumn =
                PhysicalColumn.of("test_int", BasicType.INT_TYPE, null, null, true, null, null);

        BasicTypeDefine intDefine = converter.reconvert(intColumn);
        assertNotNull(intDefine);
        assertEquals("test_int", intDefine.getName());
        assertEquals("INTEGER", intDefine.getColumnType());
        assertEquals("INTEGER", intDefine.getDataType());

        // Test reconvert DECIMAL column
        Column decimalColumn =
                PhysicalColumn.of(
                        "test_decimal", new DecimalType(10, 2), null, null, true, null, null);

        BasicTypeDefine decimalDefine = converter.reconvert(decimalColumn);
        assertNotNull(decimalDefine);
        assertEquals("test_decimal", decimalDefine.getName());
        assertEquals("DECIMAL(10,2)", decimalDefine.getColumnType());
        assertEquals("DECIMAL", decimalDefine.getDataType());
        assertEquals(10L, decimalDefine.getPrecision());
        assertEquals(2, decimalDefine.getScale());
    }

    @Test
    public void testIdentifier() {
        SnowflakeTypeConverter converter = SnowflakeTypeConverter.INSTANCE;
        assertEquals("SnowflakeFile", converter.identifier());
    }
}
