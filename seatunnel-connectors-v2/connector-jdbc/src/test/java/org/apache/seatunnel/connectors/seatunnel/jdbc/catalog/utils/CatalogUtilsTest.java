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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CatalogUtilsTest {

    @Test
    public void testGetCatalogTableShouldKeepFirstDuplicateMetadataColumn() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(3);

        CatalogTable catalogTable =
                CatalogUtils.getCatalogTable(
                        metadata,
                        (resultSetMetaData, index) -> buildColumn(index),
                        "select sec_cde, sec_cde, post_cde from test_table");

        Assertions.assertEquals(2, catalogTable.getTableSchema().getColumns().size());
        Assertions.assertEquals(
                "sec_cde", catalogTable.getTableSchema().getColumns().get(0).getName());
        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                catalogTable.getTableSchema().getColumns().get(0).getDataType());
        Assertions.assertEquals(
                "post_cde", catalogTable.getTableSchema().getColumns().get(1).getName());
    }

    private static Column buildColumn(int index) {
        switch (index) {
            case 1:
                return PhysicalColumn.of(
                        "sec_cde",
                        BasicType.STRING_TYPE,
                        8L,
                        true,
                        null,
                        null,
                        "varchar(8)",
                        false,
                        false,
                        null,
                        null,
                        null);
            case 2:
                return PhysicalColumn.of(
                        "sec_cde",
                        BasicType.LONG_TYPE,
                        null,
                        true,
                        null,
                        null,
                        "bigint",
                        false,
                        false,
                        null,
                        null,
                        null);
            case 3:
                return PhysicalColumn.of(
                        "post_cde",
                        BasicType.STRING_TYPE,
                        50L,
                        true,
                        null,
                        null,
                        "varchar(50)",
                        false,
                        false,
                        null,
                        null,
                        null);
            default:
                throw new IllegalArgumentException("Unsupported column index: " + index);
        }
    }
}
