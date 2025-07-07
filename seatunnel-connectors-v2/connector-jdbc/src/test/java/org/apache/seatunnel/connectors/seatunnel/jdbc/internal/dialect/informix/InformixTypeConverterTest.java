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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InformixTypeConverterTest {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            InformixTypeConverter.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BOOLEAN")
                        .dataType("BOOLEAN")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertShort() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SMALLINT")
                        .dataType("SMALLINT")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInteger() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("INT").dataType("INT").build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTEGER")
                        .dataType("INTEGER")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SERIAL")
                        .dataType("SERIAL")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertLong() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("INT8").dataType("INT8").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BIGSERIAL")
                        .dataType("BIGSERIAL")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SERIAL8")
                        .dataType("SERIAL8")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("FLOAT")
                        .dataType("FLOAT")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("REAL").dataType("REAL").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SMALLFLOAT")
                        .dataType("SMALLFLOAT")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DOUBLE PRECISION")
                        .dataType("DOUBLE PRECISION")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .dataType("DECIMAL")
                        .precision(0L)
                        .scale(0)
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(
                new DecimalType(InformixTypeConverter.DEFAULT_PRECISION, 0), column.getDataType());
        Assertions.assertEquals("DECIMAL(32,0)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .dataType("DECIMAL")
                        .precision(31L)
                        .scale(10)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(31, 10), column.getDataType());
        Assertions.assertEquals("DECIMAL(31,10)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .dataType("DEC")
                        .precision(31L)
                        .scale(10)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(31, 10), column.getDataType());
        Assertions.assertEquals("DECIMAL(31,10)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMERIC")
                        .dataType("NUMERIC")
                        .precision(31L)
                        .scale(10)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(31, 10), column.getDataType());
        Assertions.assertEquals("DECIMAL(31,10)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .dataType("MONEY")
                        .precision(31L)
                        .scale(0)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(31, 0), column.getDataType());
        Assertions.assertEquals("DECIMAL(31,0)", column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHAR")
                        .dataType("CHAR")
                        .length(1L)
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("CHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER")
                        .dataType("CHARACTER")
                        .length(1L)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("CHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NCHAR")
                        .dataType("NCHAR")
                        .length(1L)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("NCHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NVARCHAR")
                        .dataType("NVARCHAR")
                        .length(1L)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("NVARCHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARCHAR")
                        .dataType("VARCHAR")
                        .length(1L)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("VARCHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("LVARCHAR")
                        .dataType("LVARCHAR")
                        .length(1L)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("LVARCHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("TEXT").dataType("TEXT").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("IDSSECURITYLABEL")
                        .dataType("IDSSECURITYLABEL")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("CLOB").dataType("CLOB").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("BSON").dataType("BSON").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBytes() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BYTE")
                        .dataType("BYTE")
                        .length(1L)
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("BLOB").dataType("BLOB").build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("DATE").dataType("DATE").build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DATETIME YEAR TO FRACTION")
                        .dataType("DATETIME YEAR TO FRACTION")
                        .build();
        Column column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DATETIME YEAR TO FRACTION(5)")
                        .dataType("DATETIME YEAR TO FRACTION(5)")
                        .scale(5)
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DATETIME YEAR TO DAY")
                        .dataType("DATETIME YEAR TO DAY")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DATETIME HOUR TO FRACTION")
                        .dataType("DATETIME HOUR TO FRACTION")
                        .build();
        column = InformixTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testReconvertUnsupported() {
        Column column =
                PhysicalColumn.of(
                        "test",
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        (Long) null,
                        true,
                        null,
                        null);
        try {
            InformixTypeConverter.INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.BOOLEAN, typeDefine.getDataType());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.INT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.DOUBLE_PRECISION, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DOUBLE_PRECISION, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        InformixTypeConverter.DECIMAL,
                        InformixTypeConverter.DEFAULT_PRECISION,
                        InformixTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();
        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", InformixTypeConverter.DECIMAL, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DECIMAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.BYTE, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.BYTE, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(InformixTypeConverter.MAX_BYTES_LENGTH - 1)
                        .build();
        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.BYTE, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.BYTE, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(InformixTypeConverter.MAX_BYTES_LENGTH)
                        .build();
        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.BLOB, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.BLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.TEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength((long) (InformixTypeConverter.MAX_LVARCHAR_LENGTH - 1))
                        .build();

        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", InformixTypeConverter.LVARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.LVARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength((long) InformixTypeConverter.MAX_TEXT_LENGTH - 1)
                        .build();

        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.TEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(InformixTypeConverter.MAX_TEXT_LENGTH)
                        .build();

        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.CLOB, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.CLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(InformixTypeConverter.DATE, typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("DATETIME YEAR TO FRACTION", typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DATETIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(5)
                        .build();

        typeDefine = InformixTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("DATETIME YEAR TO FRACTION(%s)", column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(InformixTypeConverter.DATETIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }
}
