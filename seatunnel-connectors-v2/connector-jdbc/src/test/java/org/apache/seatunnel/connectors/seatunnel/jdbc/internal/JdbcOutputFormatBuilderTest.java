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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.FieldNamedPreparedStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JdbcOutputFormatBuilderTest {

    @Test
    public void testKeyExtractor() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        SeaTunnelRowType pkType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        int[] pkFields = Arrays.stream(pkType.getFieldNames()).mapToInt(rowType::indexOf).toArray();

        SeaTunnelRow insertRow = new SeaTunnelRow(new Object[] {1, "a", 60});
        insertRow.setTableId("test");
        insertRow.setRowKind(RowKind.INSERT);
        SeaTunnelRow updateBefore = new SeaTunnelRow(new Object[] {1, "a"});
        updateBefore.setTableId("test");
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow updateAfter = new SeaTunnelRow(new Object[] {1, "b"});
        updateAfter.setTableId("test");
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        SeaTunnelRow deleteRow = new SeaTunnelRow(new Object[] {1});
        deleteRow.setTableId("test");
        deleteRow.setRowKind(RowKind.DELETE);

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor =
                JdbcOutputFormatBuilder.createKeyExtractor(pkFields);
        keyExtractor.apply(insertRow);

        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(insertRow));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateBefore));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateAfter));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(deleteRow));

        updateBefore.setTableId("test1");
        Assertions.assertNotEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateBefore));
        updateAfter.setField(0, "2");
        Assertions.assertNotEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateAfter));
    }

    @Test
    public void testDeleteStatementParameterParsing() {
        // Test case for issue: DELETE statement parameter parsing with quoted identifiers
        // This tests the fix for: mateid doesn't exist in the parameters of SQL statement
        String deleteSQL =
                "DELETE FROM `qa_sink`.`murmur-64-source` WHERE `mateid` = :mateid AND `col1` = :col1";
        String[] pkNames = {"mateid", "col1"};

        Map<String, List<Integer>> parameterMap = new HashMap<>();
        String parsedSQL = FieldNamedPreparedStatement.parseNamedStatement(deleteSQL, parameterMap);

        // Verify that all parameters are correctly parsed
        Assertions.assertTrue(
                parameterMap.containsKey("mateid"), "Parameter 'mateid' should be found");
        Assertions.assertTrue(parameterMap.containsKey("col1"), "Parameter 'col1' should be found");
        Assertions.assertEquals(2, parameterMap.size(), "Should have exactly 2 parameters");

        // Verify that the parsed SQL has placeholders
        Assertions.assertTrue(parsedSQL.contains("?"), "Parsed SQL should contain placeholders");
        Assertions.assertFalse(
                parsedSQL.contains(":mateid"), "Parsed SQL should not contain named parameters");
        Assertions.assertFalse(
                parsedSQL.contains(":col1"), "Parsed SQL should not contain named parameters");
    }
}
