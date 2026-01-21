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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostgresCopyBatchStatementExecutorTest {

    /*
     * Test CSV escaping for JSON string containing double quotes.
     * This is the primary use case that triggered the fix.
     * Input: ["创业板业务控制标志","创业板业务-注册制"]
     * Expected: Inner quotes are doubled per CSV spec, field is wrapped with quotes.
     */
    @Test
    public void testCsvEscapingForJsonString() throws Exception {
        String value = "[\"创业板业务控制标志\",\"创业板业务-注册制\"]";
        String out = renderSingleStringColumn(value);
        Assertions.assertEquals("\"[\"\"创业板业务控制标志\"\",\"\"创业板业务-注册制\"\"]\"\n", out);
    }

    /*
     * Test CSV escaping when value itself contains outer quotes.
     * Input: "zhangsan"
     * Expected: Each quote is doubled, field is wrapped with quotes.
     */
    @Test
    public void testCsvEscapingForQuotedStringValue() throws Exception {
        String out = renderSingleStringColumn("\"zhangsan\"");
        System.out.println("\"\"\"zhangsan\"\"\"\n");
        Assertions.assertEquals("\"\"\"zhangsan\"\"\"\n", out);
    }

    /*
     * Test CSV escaping for string containing comma.
     * Comma triggers field quoting per CSV spec.
     */
    @Test
    public void testCsvEscapingForComma() throws Exception {
        String out = renderSingleStringColumn("a,b");
        Assertions.assertEquals("\"a,b\"\n", out);
    }

    /*
     * Test that null byte (\u0000) is removed from string.
     * PostgreSQL does not support null byte in text fields.
     */
    @Test
    public void testNullByteRemovedInString() throws Exception {
        String out = renderSingleStringColumn("a\u0000b");
        Assertions.assertEquals("\"ab\"\n", out);
    }

    /*
     * Test simple string without special characters.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testSimpleString() throws Exception {
        String out = renderSingleStringColumn("simple");
        Assertions.assertEquals("\"simple\"\n", out);
    }

    /*
     * Test empty string handling.
     * Empty string is rendered as empty quoted field.
     */
    @Test
    public void testEmptyString() throws Exception {
        String out = renderSingleStringColumn("");
        Assertions.assertEquals("\"\"\n", out);
    }

    /*
     * Test string with only quotes.
     */
    @Test
    public void testOnlyQuotes() throws Exception {
        String out = renderSingleStringColumn("\"\"");
        Assertions.assertEquals("\"\"\"\"\"\"\n", out);
    }

    /*
     * Test string with newline character.
     * Newline triggers field quoting per CSV spec.
     */
    @Test
    public void testStringWithNewline() throws Exception {
        String out = renderSingleStringColumn("line1\nline2");
        Assertions.assertEquals("\"line1\nline2\"\n", out);
    }

    /*
     * Test string with tab character.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testStringWithTab() throws Exception {
        String out = renderSingleStringColumn("col1\tcol2");
        Assertions.assertEquals("\"col1\tcol2\"\n", out);
    }

    /*
     * Test string with backslash.
     * Backslash should not require special escaping in CSV format.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testStringWithBackslash() throws Exception {
        String out = renderSingleStringColumn("path\\to\\file");
        Assertions.assertEquals("\"path\\to\\file\"\n", out);
    }

    /*
     * Test string with multiple null bytes.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testMultipleNullBytes() throws Exception {
        String out = renderSingleStringColumn("a\u0000b\u0000c");
        Assertions.assertEquals("\"abc\"\n", out);
    }

    /*
     * Test null value handling.
     */
    @Test
    public void testNullValue() throws Exception {
        PostgresCopyBatchStatementExecutor executor = new PostgresCopyBatchStatementExecutor();
        executor.init(TablePath.of("db", "public", "t"), buildSingleStringSchema());
        executor.prepareStatements(null);
        executor.addToBatch(new SeaTunnelRow(new Object[] {null}));
        executor.csvPrinter.flush();
        String out = normalizeLineEndings(executor.csvPrinter.getOut().toString());
        Assertions.assertEquals("\n", out);
    }

    /*
     * Test multi-field row with mixed special characters.
     * This verifies that each field is independently escaped.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testMultiFieldRow() throws Exception {
        PostgresCopyBatchStatementExecutor executor = new PostgresCopyBatchStatementExecutor();
        executor.init(TablePath.of("db", "public", "t"), buildMultiColumnSchema());
        executor.prepareStatements(null);

        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {1, "simple", "with,comma", "with\"quote", null});

        executor.addToBatch(row);
        executor.csvPrinter.flush();
        String out = normalizeLineEndings(executor.csvPrinter.getOut().toString());

        Assertions.assertEquals("\"1\",\"simple\",\"with,comma\",\"with\"\"quote\",\n", out);
    }

    /*
     * Test complex real-world scenario with JSON-like data in multiple fields.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testComplexJsonLikeData() throws Exception {
        PostgresCopyBatchStatementExecutor executor = new PostgresCopyBatchStatementExecutor();
        executor.init(TablePath.of("db", "public", "t"), buildMultiColumnSchema());
        executor.prepareStatements(null);

        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            100,
                            "{\"key\":\"value\"}",
                            "[\"item1\",\"item2\"]",
                            "normal,text",
                            "simple"
                        });

        executor.addToBatch(row);
        executor.csvPrinter.flush();
        String out = normalizeLineEndings(executor.csvPrinter.getOut().toString());

        Assertions.assertEquals(
                "\"100\",\"{\"\"key\"\":\"\"value\"\"}\",\"[\"\"item1\"\",\"\"item2\"\"]\",\"normal,text\",\"simple\"\n",
                out);
    }

    /*
     * Test batch with multiple rows to ensure CSV consistency.
     * CSVFormat.POSTGRESQL_CSV wraps all fields with quotes.
     */
    @Test
    public void testMultipleRows() throws Exception {
        PostgresCopyBatchStatementExecutor executor = new PostgresCopyBatchStatementExecutor();
        executor.init(TablePath.of("db", "public", "t"), buildSingleStringSchema());
        executor.prepareStatements(null);

        executor.addToBatch(new SeaTunnelRow(new Object[] {"row1"}));
        executor.addToBatch(new SeaTunnelRow(new Object[] {"row,2"}));
        executor.addToBatch(new SeaTunnelRow(new Object[] {"row\"3"}));
        executor.addToBatch(new SeaTunnelRow(new Object[] {null}));

        executor.csvPrinter.flush();
        String out = normalizeLineEndings(executor.csvPrinter.getOut().toString());

        Assertions.assertEquals("\"row1\"\n\"row,2\"\n\"row\"\"3\"\n\n", out);
    }

    /*
     * Test that the fix prevents double escaping.
     * Before fix: quotes were manually escaped then escaped again by CSVPrinter.
     * After fix: only CSVPrinter performs escaping, resulting in correct output.
     */
    @Test
    public void testNoDoubleEscaping() throws Exception {
        String value = "a\"b";
        String out = renderSingleStringColumn(value);

        // Correct: a"b becomes "a""b" (one level of escaping)
        Assertions.assertEquals("\"a\"\"b\"\n", out);

        // If double escaping occurred, it would be: "a""""b" (wrong!)
        Assertions.assertNotEquals("\"a\"\"\"\"b\"\n", out);
    }

    private String renderSingleStringColumn(String value) throws Exception {
        PostgresCopyBatchStatementExecutor executor = new PostgresCopyBatchStatementExecutor();
        executor.init(TablePath.of("db", "public", "t"), buildSingleStringSchema());
        executor.prepareStatements(null);
        executor.addToBatch(new SeaTunnelRow(new Object[] {value}));
        executor.csvPrinter.flush();
        return normalizeLineEndings(executor.csvPrinter.getOut().toString());
    }

    private TableSchema buildSingleStringSchema() {
        return TableSchema.builder()
                .column(
                        new PhysicalColumn(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null))
                .build();
    }

    private TableSchema buildMultiColumnSchema() {
        return TableSchema.builder()
                .column(new PhysicalColumn("id", BasicType.INT_TYPE, null, null, true, null, null))
                .column(
                        new PhysicalColumn(
                                "col1", BasicType.STRING_TYPE, null, null, true, null, null))
                .column(
                        new PhysicalColumn(
                                "col2", BasicType.STRING_TYPE, null, null, true, null, null))
                .column(
                        new PhysicalColumn(
                                "col3", BasicType.STRING_TYPE, null, null, true, null, null))
                .column(
                        new PhysicalColumn(
                                "col4", BasicType.STRING_TYPE, null, null, true, null, null))
                .build();
    }

    private String normalizeLineEndings(String value) {
        return value.replace("\r\n", "\n");
    }
}
