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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;

import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.List;

public class CastFunction {

    public static final String DECIMAL = "DECIMAL";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String LONG = "LONG";
    public static final String BYTE = "BYTE";
    public static final String BYTES = "BYTES";
    public static final String BINARY = "BINARY";
    public static final String DOUBLE = "DOUBLE";
    public static final String FLOAT = "FLOAT";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String DATETIME = "DATETIME";
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String BOOLEAN = "BOOLEAN";

    public static SeaTunnelDataType<?> getCastType(SqlType originType, ColDataType colDataType) {
        String dataType = colDataType.getDataType();
        switch (dataType.toUpperCase()) {
            case DECIMAL:
                List<String> ps = colDataType.getArgumentsStringList();
                return new DecimalType(Integer.parseInt(ps.get(0)), Integer.parseInt(ps.get(1)));
            case VARCHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case BYTE:
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BYTES:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported CAST FROM %s AS type: %s",
                                originType.name(), dataType));
        }
    }
}
