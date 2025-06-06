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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.analyticdb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference
// https://help.aliyun.com/zh/analyticdb/analyticdb-for-postgresql/developer-reference/data-types
@Slf4j
@AutoService(TypeConverter.class)
public class AnalyticDBPostgresTypeConverter extends PostgresTypeConverter {

    public static final String ANALYTICDB_BOOLEAN = "boolean";
    public static final String ANALYTICDB_SMALLINT = "smallint";
    public static final String ANALYTICDB_INTEGER = "integer";
    public static final String ANALYTICDB_SERIAL = "serial4";
    public static final String ANALYTICDB_BIGSERIAL = "serial8";
    public static final String ANALYTICDB_BIGINT = "bigint";
    public static final String ANALYTICDB_REAL = "real";
    public static final String ANALYTICDB_DOUBLE_PRECISION = "double precision";
    public static final String ANALYTICDB_DECIMAL = "decimal";
    public static final String ANALYTICDB_TIME = "time without time zone";
    public static final String ANALYTICDB_TIMETZ = "time with time zone";
    public static final String ANALYTICDB_TIMESTAMP = "timestamp without time zone";
    public static final String ANALYTICDB_TIMESTAMPTZ = "timestamp with time zone";
    public static final String ANALYTICDB_CHAR = "char";
    public static final String ANALYTICDB_BIT = "bit";
    public static final String ANALYTICDB_VARBIT = "varbit";
    public static final String ANALYTICDB_BIT_VARYING = "bit varying";

    @Override
    public String identifier() {
        return DatabaseIdentifier.ANALYTIC_DB_PG;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        String dataType = typeDefine.getDataType().toLowerCase();
        switch (dataType) {
            case ANALYTICDB_INTEGER:
            case ANALYTICDB_SERIAL:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_INTEGER).build());
            case ANALYTICDB_BIGSERIAL:
            case ANALYTICDB_BIGINT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BIGINT).build());
            case ANALYTICDB_BOOLEAN:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BOOLEAN).build());
            case ANALYTICDB_REAL:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_REAL).build());
            case ANALYTICDB_DOUBLE_PRECISION:
                return super.convert(
                        typeDefine
                                .toBuilder()
                                .dataType(PostgresTypeConverter.PG_DOUBLE_PRECISION)
                                .build());
            case ANALYTICDB_TIME:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_TIME).build());
            case ANALYTICDB_TIMETZ:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_TIME_TZ).build());
            case ANALYTICDB_TIMESTAMPTZ:
                return super.convert(
                        typeDefine
                                .toBuilder()
                                .dataType(PostgresTypeConverter.PG_TIMESTAMP_TZ)
                                .build());
            case ANALYTICDB_TIMESTAMP:
                return super.convert(
                        typeDefine
                                .toBuilder()
                                .dataType(PostgresTypeConverter.PG_TIMESTAMP)
                                .build());
            case ANALYTICDB_DECIMAL:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_NUMERIC).build());
            case ANALYTICDB_CHAR:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_CHAR).build());
            case ANALYTICDB_SMALLINT:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_SMALLINT).build());
            case ANALYTICDB_BIT:
            case ANALYTICDB_VARBIT:
            case ANALYTICDB_BIT_VARYING:
                return super.convert(
                        typeDefine.toBuilder().dataType(PostgresTypeConverter.PG_BYTEA).build());
            default:
                return super.convert(typeDefine);
        }
    }
}
