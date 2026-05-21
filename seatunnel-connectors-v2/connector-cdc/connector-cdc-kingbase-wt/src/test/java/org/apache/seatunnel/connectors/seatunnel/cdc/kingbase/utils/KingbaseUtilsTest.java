package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class KingbaseUtilsTest {
    @Test
    public void testSplitScanQuery() {
        String splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false,
                        null,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true,
                        null,
                        false);
        Assertions.assertEquals("SELECT * FROM \"schema1\".\"table1\"", splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false,
                        null,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" <= ? AND NOT (\"id\" = ?)",
                splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true,
                        null,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" >= ?", splitScanSQL);
        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true,
                        null,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" IS NULL", splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"name"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        false,
                        true,
                        null,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"name\" IS NULL", splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"name"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        true,
                        true,
                        null,
                        false);
        Assertions.assertEquals("SELECT * FROM \"schema1\".\"table1\"", splitScanSQL);

        splitScanSQL =
                KingbaseUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"name"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        false,
                        false,
                        new Object[] {10},
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE (ABS(HASHTEXT(\"name\")) % 10) = ?",
                splitScanSQL);
    }
}
