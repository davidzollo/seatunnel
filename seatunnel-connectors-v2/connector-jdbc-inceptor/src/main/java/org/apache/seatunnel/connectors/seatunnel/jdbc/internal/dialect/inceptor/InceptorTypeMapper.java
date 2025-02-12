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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class InceptorTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return InceptorTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String columnType = metadata.getColumnTypeName(colIndex);
        String nativeColumnType = columnType;
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        if (Arrays.asList("CHAR", "VARCHAR", "VARCHAR2").contains(columnType)) {
            long octetLength = TypeDefineUtils.charTo4ByteLength(precision);
            precision = Math.max(precision, octetLength);
            switch (columnType) {
                case "CHAR":
                    nativeColumnType = "char(" + precision + ")";
                    break;
                case "VARCHAR":
                case "VARCHAR2":
                    nativeColumnType = "varchar(" + precision + ")";
                    break;
            }
        }

        if (columnType.equalsIgnoreCase("decimal")) {
            nativeColumnType = "decimal(" + precision + "," + scale + ")";
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeColumnType)
                        .dataType(columnType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
