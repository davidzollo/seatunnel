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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for SnowflakeDataTypeConvertor to verify consistent timestamp type handling and proper
 * data type conversions after fixes.
 */
public class SnowflakeDataTypeConvertorTest {

    private final SnowflakeDataTypeConvertor convertor = new SnowflakeDataTypeConvertor();

    @Test
    public void testGetIdentity() {
        assertEquals(DatabaseIdentifier.SNOWFLAKE, convertor.getIdentity());
    }

    @Test
    public void testTimestampTypesConsistency() {
        // Test all timestamp variations with correct underscores (TIMESTAMP_*)
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                convertor.toSeaTunnelType("test_field", "TIMESTAMP_LTZ"));
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                convertor.toSeaTunnelType("test_field", "TIMESTAMP_NTZ"));
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                convertor.toSeaTunnelType("test_field", "TIMESTAMP_TZ"));
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                convertor.toSeaTunnelType("test_field", "TIMESTAMP"));
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                convertor.toSeaTunnelType("test_field", "DATE_TIME"));
    }

    @Test
    public void testGeographyAndGeometryMapping() {
        // Test GEOGRAPHY maps to byte array (as per SnowflakeDataTypeConvertor)
        assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                convertor.toSeaTunnelType("test_field", "GEOGRAPHY"));

        // Test GEOMETRY maps to String (as per SnowflakeDataTypeConvertor)
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "GEOMETRY"));
    }

    @Test
    public void testDecimalTypeWithPrecisionScale() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("precision", 15);
        properties.put("scale", 2);

        DecimalType result =
                (DecimalType) convertor.toSeaTunnelType("test_field", "DECIMAL", properties);
        assertEquals(15, result.getPrecision());
        assertEquals(2, result.getScale());

        // Test default values
        DecimalType defaultResult = (DecimalType) convertor.toSeaTunnelType("test_field", "NUMBER");
        assertEquals(10, defaultResult.getPrecision());
        assertEquals(0, defaultResult.getScale());
    }

    @Test
    public void testIntegerTypes() {
        assertEquals(BasicType.SHORT_TYPE, convertor.toSeaTunnelType("test_field", "SMALLINT"));
        assertEquals(BasicType.SHORT_TYPE, convertor.toSeaTunnelType("test_field", "TINYINT"));
        assertEquals(BasicType.SHORT_TYPE, convertor.toSeaTunnelType("test_field", "BYTEINT"));
        assertEquals(BasicType.INT_TYPE, convertor.toSeaTunnelType("test_field", "INTEGER"));
        assertEquals(BasicType.INT_TYPE, convertor.toSeaTunnelType("test_field", "INT"));
        assertEquals(BasicType.LONG_TYPE, convertor.toSeaTunnelType("test_field", "BIGINT"));
    }

    @Test
    public void testFloatingPointTypes() {
        assertEquals(BasicType.FLOAT_TYPE, convertor.toSeaTunnelType("test_field", "REAL"));
        assertEquals(BasicType.FLOAT_TYPE, convertor.toSeaTunnelType("test_field", "FLOAT4"));
        assertEquals(BasicType.DOUBLE_TYPE, convertor.toSeaTunnelType("test_field", "DOUBLE"));
        assertEquals(
                BasicType.DOUBLE_TYPE, convertor.toSeaTunnelType("test_field", "DOUBLE PRECISION"));
        assertEquals(BasicType.DOUBLE_TYPE, convertor.toSeaTunnelType("test_field", "FLOAT8"));
        assertEquals(BasicType.DOUBLE_TYPE, convertor.toSeaTunnelType("test_field", "FLOAT"));
    }

    @Test
    public void testStringTypes() {
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "CHAR"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "CHARACTER"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "VARCHAR"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "STRING"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "TEXT"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "VARIANT"));
        assertEquals(BasicType.STRING_TYPE, convertor.toSeaTunnelType("test_field", "OBJECT"));
    }

    @Test
    public void testBinaryTypes() {
        assertEquals(
                PrimitiveByteArrayType.INSTANCE, convertor.toSeaTunnelType("test_field", "BINARY"));
        assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                convertor.toSeaTunnelType("test_field", "VARBINARY"));
    }

    @Test
    public void testDateTimeTypes() {
        assertEquals(
                LocalTimeType.LOCAL_DATE_TYPE, convertor.toSeaTunnelType("test_field", "DATE"));
        assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE, convertor.toSeaTunnelType("test_field", "TIME"));
    }

    @Test
    public void testBooleanType() {
        assertEquals(BasicType.BOOLEAN_TYPE, convertor.toSeaTunnelType("test_field", "BOOLEAN"));
    }

    @Test
    public void testReverseConversion() {
        // Test SeaTunnel types back to Snowflake types
        assertEquals(
                "SMALLINT", convertor.toConnectorType("test_field", BasicType.SHORT_TYPE, null));
        assertEquals("INTEGER", convertor.toConnectorType("test_field", BasicType.INT_TYPE, null));
        assertEquals("BIGINT", convertor.toConnectorType("test_field", BasicType.LONG_TYPE, null));
        assertEquals(
                "DECIMAL", convertor.toConnectorType("test_field", new DecimalType(10, 2), null));
        assertEquals("FLOAT4", convertor.toConnectorType("test_field", BasicType.FLOAT_TYPE, null));
        assertEquals(
                "DOUBLE PRECISION",
                convertor.toConnectorType("test_field", BasicType.DOUBLE_TYPE, null));
        assertEquals(
                "BOOLEAN", convertor.toConnectorType("test_field", BasicType.BOOLEAN_TYPE, null));
        assertEquals("TEXT", convertor.toConnectorType("test_field", BasicType.STRING_TYPE, null));
        assertEquals(
                "DATE",
                convertor.toConnectorType("test_field", LocalTimeType.LOCAL_DATE_TYPE, null));
        assertEquals(
                "VARBINARY",
                convertor.toConnectorType("test_field", PrimitiveByteArrayType.INSTANCE, null));
        assertEquals(
                "TIME",
                convertor.toConnectorType("test_field", LocalTimeType.LOCAL_TIME_TYPE, null));
        assertEquals(
                "TIMESTAMP",
                convertor.toConnectorType("test_field", LocalTimeType.LOCAL_DATE_TIME_TYPE, null));
    }

    @Test
    public void testUnsupportedType() {
        assertThrows(
                SeaTunnelRuntimeException.class,
                () -> {
                    convertor.toSeaTunnelType("test_field", "UNSUPPORTED_TYPE");
                });
    }
}
