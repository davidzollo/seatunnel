package org.apache.seatunnel.connectors.cdc.base.config;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnIncludeListBuilderTest {

    @Test
    void testSingleTableWithColumns() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> cols = new ArrayList<>();
        cols.add("col1");
        cols.add("col2");
        cols.add("col3");
        map.put("mydb.my_table", cols);

        String regex =
                JdbcSourceConfigFactory.buildColumnIncludeList(
                        map,
                        Lists.newArrayList("mydb"),
                        Lists.newArrayList("mydb.my_table"),
                        false);

        assertEquals("mydb.my_table.(col1|col2|col3)", regex);
        assertTrue("mydb.my_table.col1".matches(regex));
        assertTrue("mydb.my_table.col2".matches(regex));
        assertFalse("mydb.my_table.other".matches(regex));
    }

    @Test
    void testSingleTableAllColumns() {
        Map<String, List<String>> map = new HashMap<>();
        map.put("mydb.my_table", new ArrayList<>()); // empty => match all

        String regex =
                JdbcSourceConfigFactory.buildColumnIncludeList(
                        map,
                        Lists.newArrayList("mydb"),
                        Lists.newArrayList("mydb.my_table"),
                        false);

        assertEquals("mydb.my_table.*", regex);
        assertTrue("mydb.my_table.anything".matches(regex));
        assertFalse("yourdb.my_table.anything".matches(regex));
    }

    @Test
    void testMultipleTablesMixed() {
        Map<String, List<String>> map = new LinkedHashMap<>();

        List<String> cols1 = new ArrayList<>();
        cols1.add("a");
        cols1.add("b");
        map.put("db1.tbl1", cols1);

        String regex =
                JdbcSourceConfigFactory.buildColumnIncludeList(
                        map,
                        Lists.newArrayList("db1", "db2"),
                        Lists.newArrayList("db2.tbl2"),
                        false);
        String expected = "db1.tbl1.(a|b),db2.tbl2.*";
        assertEquals(expected, regex);
    }
}
