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

package org.apache.seatunnel.connectors.seatunnel.sapbw.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.sapbw.source.SAPBWSourceFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class SAPBWTypeConverter implements TypeConverter<BasicTypeDefine<String>> {

    public static SAPBWTypeConverter INSTANCE = new SAPBWTypeConverter();

    private static final String ACCP = "ACCP";
    private static final String CHAR = "CHAR";
    private static final String CLNT = "CLNT";
    private static final String CUKY = "CUKY";
    private static final String CURR = "CURR";
    private static final String DATS = "DATS";
    private static final String DEC = "DEC";
    private static final String DF16_RAW = "DF16_RAW";
    private static final String DF16_SCL = "DF16_SCL";
    private static final String DF34_RAW = "DF34_RAW";
    private static final String DF34_SCL = "DF34_SCL";
    private static final String FLTP = "FLTP";
    private static final String INT1 = "INT1";
    private static final String INT2 = "INT2";
    private static final String INT4 = "INT4";
    private static final String LANG = "LANG";
    private static final String LCHR = "LCHR";
    private static final String LRAW = "LRAW";
    private static final String NUMC = "NUMC";
    private static final String PREC = "PREC";
    private static final String QUAN = "QUAN";
    private static final String RAW = "RAW";
    private static final String RAWSTRING = "RAWSTRING";
    private static final String SSTRING = "SSTRING";
    private static final String STRING = "STRING";
    private static final String TIMS = "TIMS";
    private static final String UNIT = "UNIT";

    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 18;

    @Override
    public String identifier() {
        return SAPBWSourceFactory.IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine<String> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String dataType = typeDefine.getDataType();
        switch (dataType) {
            case ACCP:
            case CHAR:
            case CLNT:
            case CUKY:
            case LANG:
            case LCHR:
            case NUMC:
            case SSTRING:
            case STRING:
            case UNIT:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case CURR:
            case DEC:
            case DF16_RAW:
            case DF16_SCL:
            case DF34_RAW:
            case DF34_SCL:
            case PREC:
            case QUAN:
                DecimalType decimalType;
                if (typeDefine.getPrecision() > DEFAULT_PRECISION) {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                } else {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(),
                                    typeDefine.getScale() == null
                                            ? 0
                                            : typeDefine.getScale().intValue());
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case DATS:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case FLTP:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case INT1:
            case INT2:
            case INT4:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case LRAW:
            case RAW:
            case RAWSTRING:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case TIMS:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        SAPBWSourceFactory.IDENTIFIER, dataType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<String> reconvert(Column column) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
