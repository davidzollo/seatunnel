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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JdbcStarRocksTypeConverterTest {

    @Test
    void testDecimalWithNumberSuffix() {
        JdbcStarRocksTypeConverter converter = new JdbcStarRocksTypeConverter();
        BasicTypeDefine define =
                BasicTypeDefine.builder()
                        .name("DECIMAL_TYPE")
                        .dataType("DECIMAL128")
                        .columnType("DECIMAL128(10,2)")
                        .precision(10L)
                        .scale(2)
                        .build();
        Column column = converter.convert(define);
        Assertions.assertEquals(SqlType.DECIMAL, column.getDataType().getSqlType());
    }
}
