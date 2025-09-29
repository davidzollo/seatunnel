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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresJdbcRowConverter;

import com.huawei.gauss200.jdbc.util.PGobject;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GaussDBJdbcRowConverter extends PostgresJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.GAUSSDB;
    }

    @Override
    public Object[] convertToArray(
            ResultSet rs,
            int resultSetIndex,
            SeaTunnelDataType<?> seaTunnelDataType,
            String fieldName)
            throws SQLException {
        Array array = rs.getArray(resultSetIndex);
        if (array != null) {
            Object[] elementArr = (Object[]) array.getArray();
            SeaTunnelDataType<?> elementType =
                    ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
            switch (elementType.getSqlType()) {
                case STRING:
                    String[] strArray = new String[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        strArray[i] = elementArr[i].toString();
                    }
                    return strArray;
                case BOOLEAN:
                    Boolean[] booArray = new Boolean[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        booArray[i] = Boolean.valueOf(elementArr[i].toString());
                    }
                    return booArray;
                case SMALLINT:
                    Short[] shortArray = new Short[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        shortArray[i] = Short.valueOf(elementArr[i].toString());
                    }
                    return shortArray;
                case INT:
                    Integer[] intArray = new Integer[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        intArray[i] = Integer.valueOf(elementArr[i].toString());
                    }
                    return intArray;
                case BIGINT:
                    Long[] longArray = new Long[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        longArray[i] = Long.valueOf(elementArr[i].toString());
                    }
                    return longArray;
                case FLOAT:
                    Float[] floatArray = new Float[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        floatArray[i] = Float.valueOf(elementArr[i].toString());
                    }
                    return floatArray;
                case DOUBLE:
                    Double[] doubleArray = new Double[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        doubleArray[i] = Double.valueOf(elementArr[i].toString());
                    }
                    return doubleArray;
                case DECIMAL:
                    BigDecimal[] decimalArray = new BigDecimal[elementArr.length];
                    for (int i = 0; i < elementArr.length; i++) {
                        String value = ((PGobject) elementArr[i]).getValue();
                        decimalArray[i] = new BigDecimal(value);
                    }
                    return decimalArray;
                default:
                    String type = String.format("Array[%s]", elementType.getSqlType());
                    throw CommonError.unsupportedDataType(converterName(), type, fieldName);
            }
        } else {
            return null;
        }
    }
}
