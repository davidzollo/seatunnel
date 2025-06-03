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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hologres;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://help.aliyun.com/zh/hologres/developer-reference/data-types
@Slf4j
@AutoService(TypeConverter.class)
public class HologresTypeConverter extends PostgresTypeConverter {

    public static final String HOLOGRES_INTEGER = "INTEGER";
    public static final String HOLOGRES_INT = "INT";
    public static final String HOLOGRES_BIGINT = "BIGINT";
    public static final String HOLOGRES_BOOLEAN = "BOOLEAN";
    public static final String HOLOGRES_REAL = "REAL";
    public static final String HOLOGRES_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String HOLOGRES_TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE";
    public static final String HOLOGRES_DECIMAL = "DECIMAL";
    public static final String HOLOGRES_CHAR = "CHAR";
    public static final String HOLOGRES_SMALLINT = "SMALLINT";
    public static final String HOLOGRES_BIT = "BIT";
    public static final String HOLOGRES_VARBIT = "VARBIT";

    @Override
    public String identifier() {
        return DatabaseIdentifier.HOLOGRES;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        String dataType = typeDefine.getDataType().toUpperCase();
        switch (dataType) {
            case HOLOGRES_INTEGER:
            case HOLOGRES_INT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_INTEGER).build());
            case HOLOGRES_BIGINT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BIGINT).build());
            case HOLOGRES_BOOLEAN:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BOOLEAN).build());
            case HOLOGRES_REAL:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_REAL).build());
            case HOLOGRES_DOUBLE_PRECISION:
                return super.convert(
                        typeDefine
                                .toBuilder()
                                .dataType(PostgresTypeConverter.PG_DOUBLE_PRECISION)
                                .build());
            case HOLOGRES_TIMESTAMPTZ:
                return super.convert(
                        typeDefine
                                .toBuilder()
                                .dataType(PostgresTypeConverter.PG_TIMESTAMP_TZ)
                                .build());
            case HOLOGRES_DECIMAL:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_NUMERIC).build());
            case HOLOGRES_CHAR:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_CHAR).build());
            case HOLOGRES_SMALLINT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_SMALLINT).build());
            case HOLOGRES_BIT:
            case HOLOGRES_VARBIT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BYTEA).build());
            default:
                return super.convert(typeDefine);
        }
    }
}
