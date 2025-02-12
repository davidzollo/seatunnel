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

package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Field;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

import java.sql.Types;

public class CustomOracleAgentValueConverter extends OracleValueConverters {

    private static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
    private static final String HEXTORAW_FUNCTION_END = "')";

    public CustomOracleAgentValueConverter(
            OracleConnectorConfig config, OracleConnection connection) {
        super(config, connection);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return (data) -> {
                    if (data instanceof String) {
                        if (isHexToRawFunctionCall((String) data)) {
                            return convertBinary(column, fieldDefn, data, binaryMode);
                        } else if (isHexString((String) data)) {
                            return hexToRawString((String) data);
                        }
                    }
                    return convertBinary(column, fieldDefn, data, binaryMode);
                };
        }

        return super.converter(column, fieldDefn);
    }

    public static boolean isHexString(String input) {
        if (input == null || input.isEmpty()) {
            return false;
        }
        return input.matches("[0-9A-Fa-f]+");
    }

    private static String hexToRawString(String b) {
        return HEXTORAW_FUNCTION_START + b + HEXTORAW_FUNCTION_END;
    }

    private boolean isHexToRawFunctionCall(String value) {
        return value != null
                && value.startsWith(HEXTORAW_FUNCTION_START)
                && value.endsWith(HEXTORAW_FUNCTION_END);
    }
}
