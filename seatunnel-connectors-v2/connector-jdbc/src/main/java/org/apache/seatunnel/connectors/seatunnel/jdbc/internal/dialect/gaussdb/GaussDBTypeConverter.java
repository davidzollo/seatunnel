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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gaussdb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

// reference https://support.huaweicloud.com/sqlreference-dws/dws_06_0009.html
@Slf4j
@AutoService(TypeConverter.class)
public class GaussDBTypeConverter extends PostgresTypeConverter {

    // GAUSSDB jdbc driver maps several alias to real type, we use real type rather than alias:

    // number type
    public static final String GAUSSDB_TINYINT = "int1";
    public static final String GAUSSDB_TINYINT_ARRAY = "_int1";

    // varchar type
    public static final String GAUSSDB_NVARCHAR2 = "nvarchar2";
    public static final String GAUSSDB_NVARCHAR = "nvarchar";
    public static final String GAUSSDB_NVARCHAR_ARRAY = "_nvarchar";
    public static final String GAUSSDB_NVARCHAR2_ARRAY = "_nvarchar2";

    // date type

    // Date and time without time zone. Accurate to the minute, the second bit is greater than or
    // equal to 30 seconds.
    public static final String GAUSSDB_SMALLDATETIME = "smalldatetime";

    // jsonb
    public static final String GAUSSDB_JSONB = "JSONB";

    public static final GaussDBTypeConverter INSTANCE = new GaussDBTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.GAUSSDB;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String gaussdbDataType = typeDefine.getDataType().toLowerCase(Locale.ROOT);
        switch (gaussdbDataType) {
            case GAUSSDB_TINYINT:
                // The tinyint is 0 ~ 255 in GAUSSDB, so need use short to storage it.
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case GAUSSDB_TINYINT_ARRAY:
                builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
                break;
            case GAUSSDB_NVARCHAR2:
            case GAUSSDB_NVARCHAR:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case GAUSSDB_NVARCHAR2_ARRAY:
            case GAUSSDB_NVARCHAR_ARRAY:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case GAUSSDB_SMALLDATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(0);
                break;
            case GAUSSDB_JSONB:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            default:
                return super.convert(typeDefine);
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType("BOOLEAN");
                builder.dataType("BOOLEAN");
                break;
            case TINYINT:
                builder.columnType("INT2");
                builder.dataType("INT2");
                break;
            case SMALLINT:
                builder.columnType("SMALLINT");
                builder.dataType("SMALLINT");
                break;
            case INT:
                builder.columnType("INTEGER");
                builder.dataType("INTEGER");
                break;
            case BIGINT:
                builder.columnType("BIGINT");
                builder.dataType("BIGINT");
                break;
            case FLOAT:
                builder.columnType("REAL");
                builder.dataType("REAL");
                break;
            case DOUBLE:
                builder.columnType("DOUBLE PRECISION");
                builder.dataType("DOUBLE PRECISION");
                break;
            case DECIMAL:
                if (column.getScale() != null && column.getScale() > 0) {
                    builder.columnType(
                            String.format(
                                    "DECIMAL(%d,%d)",
                                    column.getColumnLength() != null
                                            ? column.getColumnLength().intValue()
                                            : 38,
                                    column.getScale()));
                } else {
                    builder.columnType(
                            String.format(
                                    "DECIMAL(%d)",
                                    column.getColumnLength() != null
                                            ? column.getColumnLength().intValue()
                                            : 38));
                }
                builder.dataType("DECIMAL");
                if (column.getColumnLength() != null) {
                    builder.precision(column.getColumnLength());
                }
                if (column.getScale() != null) {
                    builder.scale(column.getScale());
                }
                break;
            case STRING:
                if (column.getColumnLength() != null && column.getColumnLength() > 0) {
                    if (column.getColumnLength() <= 4000) {
                        builder.columnType(
                                String.format("VARCHAR(%d)", column.getColumnLength().intValue()));
                        builder.dataType("VARCHAR");
                    } else {
                        builder.columnType("TEXT");
                        builder.dataType("TEXT");
                    }
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType("TEXT");
                    builder.dataType("TEXT");
                }
                break;
            case DATE:
                builder.columnType("DATE");
                builder.dataType("DATE");
                break;
            case TIME:
                builder.columnType("TIME");
                builder.dataType("TIME");
                break;
            case TIMESTAMP:
                builder.columnType("TIMESTAMP");
                builder.dataType("TIMESTAMP");
                break;
            case BYTES:
                builder.columnType("BYTEA");
                builder.dataType("BYTEA");
                break;
            case ARRAY:
                // Handle array types
                ArrayType arrayType = (ArrayType) column.getDataType();
                switch (arrayType.getElementType().getSqlType()) {
                    case STRING:
                        builder.columnType("TEXT[]");
                        builder.dataType("_text");
                        break;
                    case INT:
                        builder.columnType("INTEGER[]");
                        builder.dataType("_int4");
                        break;
                    case BIGINT:
                        builder.columnType("BIGINT[]");
                        builder.dataType("_int8");
                        break;
                    default:
                        builder.columnType("TEXT[]");
                        builder.dataType("_text");
                        break;
                }
                break;
            default:
                // Fallback to PostgreSQL converter for other types
                return super.reconvert(column);
        }

        return builder.build();
    }
}
