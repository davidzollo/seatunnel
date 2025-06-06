/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.dws.guassdb.sink.sql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.writer.SnapshotIdManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DwsGaussSqlGeneratorTest {
    private static final String delimiter = "\t";

    private static DwsGaussSqlGenerator dwsGaussSqlGenerator;

    @BeforeAll
    public static void before() {
        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, false, null, null));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, 0, false, null, null));
        columns.add(
                PhysicalColumn.of(
                        "create_time", LocalTimeType.LOCAL_DATE_TIME_TYPE, 0, false, null, null));

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.INDEX_KEY,
                                        "id",
                                        Lists.newArrayList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "id", ConstraintKey.ColumnSortType.ASC))))
                        .build();

        TableIdentifier tableIdentifier =
                TableIdentifier.of("Dws-GaussDB", "database", "public", "t_st_users");
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier, tableSchema, new HashMap<>(), new ArrayList<>(), "");
        dwsGaussSqlGenerator =
                new DwsGaussSqlGenerator(
                        "id", DwsGaussDBSinkOption.FieldIdeEnum.ORIGINAL, catalogTable, delimiter);
    }

    @Test
    void getTemporaryTableName() {
        String temporaryTableName = dwsGaussSqlGenerator.getTemporaryTableName();
        Assertions.assertEquals("st_temporary_t_st_users", temporaryTableName);
    }

    @Test
    void getTargetTableName() {
        String targetTableName = dwsGaussSqlGenerator.getTargetTableName();
        Assertions.assertEquals("t_st_users", targetTableName);
    }

    @Test
    void getCopyInTemporaryTableSql() {
        String copyInTemporaryTableSql = dwsGaussSqlGenerator.getCopyInTemporaryTableSql();
        Assertions.assertEquals(
                "COPY \"public\".\"st_temporary_t_st_users\"(id,name,age,create_time,st_snapshot_id,st_is_deleted) FROM STDIN WITH(format 'text', delimiter E'"
                        + delimiter
                        + "', noescaping 'true', compatible_illegal_chars 'true')",
                copyInTemporaryTableSql);
    }

    @Test
    void getCopyInTargetTableSql() {
        String copyInTargetTableSql = dwsGaussSqlGenerator.getCopyInTargetTableSql();
        Assertions.assertEquals(
                "COPY \"public\".\"t_st_users\"(id,name,age,create_time) FROM STDIN WITH(format 'text', delimiter E'"
                        + delimiter
                        + "', noescaping 'true', compatible_illegal_chars 'true')",
                copyInTargetTableSql);
    }

    @Test
    void getTemporaryRows() {
        List<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
        Object[] fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = 18;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        String deleteRows =
                dwsGaussSqlGenerator.getTemporaryRows(Lists.newArrayList(seaTunnelRow), true, 1L);
        Assertions.assertEquals(
                String.join(delimiter, "1", "tom", "18", "2023-09-07T10:10:10", "1", "true"),
                deleteRows);

        String upsertRows =
                dwsGaussSqlGenerator.getTemporaryRows(Lists.newArrayList(seaTunnelRow), false, 1L);
        Assertions.assertEquals(
                String.join(delimiter, "1", "tom", "18", "2023-09-07T10:10:10", "1", "false"),
                upsertRows);
    }

    @Test
    void getTargetTableRows() {
        Object[] fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = 18;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);

        String targetTableRows =
                dwsGaussSqlGenerator.getTargetTableRows(Lists.newArrayList(seaTunnelRow));
        Assertions.assertEquals(
                String.join(delimiter, "1", "tom", "18", "2023-09-07T10:10:10"), targetTableRows);

        fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = null;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        seaTunnelRow = new SeaTunnelRow(fields);

        targetTableRows = dwsGaussSqlGenerator.getTargetTableRows(Lists.newArrayList(seaTunnelRow));
        Assertions.assertEquals(
                String.join(delimiter, "1", "tom", "", "2023-09-07T10:10:10"), targetTableRows);
    }

    @Test
    void getDeleteTemporarySnapshotSql() {
        String deleteTemporarySnapshotSql =
                dwsGaussSqlGenerator.getDeleteTemporarySnapshotSql(Lists.newArrayList(1L));
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id in (1)",
                deleteTemporarySnapshotSql);
    }

    @Test
    void getDeleteTargetTableSql() {
        String deleteTargetTableSql = dwsGaussSqlGenerator.getDeleteTargetTableSql();
        Assertions.assertEquals("DELETE FROM \"public\".\"t_st_users\"", deleteTargetTableSql);
    }

    @Test
    void getDropTemporaryTableSql() {
        String dropTemporaryTableSql = dwsGaussSqlGenerator.getDropTemporaryTableSql();
        Assertions.assertEquals(
                "DROP TABLE IF EXISTS \"public\".\"st_temporary_t_st_users\"",
                dropTemporaryTableSql);
    }

    @Test
    void getCreateTemporaryTableSql() {
        String createTemporaryTableSql = dwsGaussSqlGenerator.getCreateTemporaryTableSql();
        Assertions.assertEquals(
                "CREATE TABLE IF NOT EXISTS \"public\".\"st_temporary_t_st_users\" (\n"
                        + "\"id\" int4 NOT NULL PRIMARY KEY,\n"
                        + "\"name\" text NOT NULL,\n"
                        + "\"age\" int4 NOT NULL,\n"
                        + "\"create_time\" timestamp NOT NULL,\n"
                        + "\"st_snapshot_id\" bigint,\n"
                        + "\"st_is_deleted\" boolean\n"
                        + ");",
                createTemporaryTableSql);
    }

    @Test
    void testGetTemporaryTableName() {
        Assertions.assertEquals(
                "st_temporary_t_st_users", dwsGaussSqlGenerator.getTemporaryTableName());
    }

    @Test
    void testGetTargetTableName() {
        Assertions.assertEquals("t_st_users", dwsGaussSqlGenerator.getTargetTableName());
    }

    @Test
    void testGetCopyInTemporaryTableSql() {
        Assertions.assertEquals(
                "COPY \"public\".\"st_temporary_t_st_users\"(id,name,age,create_time,st_snapshot_id,st_is_deleted) FROM STDIN WITH(format 'text', delimiter E'"
                        + delimiter
                        + "', noescaping 'true', compatible_illegal_chars 'true')",
                dwsGaussSqlGenerator.getCopyInTemporaryTableSql());
    }

    @Test
    void testGetCopyInTargetTableSql() {
        Assertions.assertEquals(
                "COPY \"public\".\"t_st_users\"(id,name,age,create_time) FROM STDIN WITH(format 'text', delimiter E'"
                        + delimiter
                        + "', noescaping 'true', compatible_illegal_chars 'true')",
                dwsGaussSqlGenerator.getCopyInTargetTableSql());
    }

    @Test
    void getMergeInTargetTableSql() {
        String mergeInTargetTableSql = dwsGaussSqlGenerator.getMergeInTargetTableSql(1L);
        Assertions.assertEquals(
                "INSERT INTO \"public\".\"t_st_users\" SELECT id,name,age,create_time FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id = 1 ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,age=EXCLUDED.age,create_time=EXCLUDED.create_time;",
                mergeInTargetTableSql);
    }

    @Test
    void getDeleteRowsInTargetTableSql() {
        String deleteRowsInTargetTableSql = dwsGaussSqlGenerator.getDeleteRowsInTargetTableSql(1L);
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"t_st_users\" WHERE id IN (SELECT id FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id = 1 AND st_is_deleted = true)",
                deleteRowsInTargetTableSql);
    }

    @Test
    void getDeleteRowsInTemporaryTableSql() {
        Long currentSnapshotId = new SnapshotIdManager().getCurrentSnapshotId();
        String deleteRowsInTemporaryTableSql =
                dwsGaussSqlGenerator.getDeleteRowsInTemporaryTableSql(currentSnapshotId);
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id = "
                        + currentSnapshotId
                        + " AND st_is_deleted = true",
                deleteRowsInTemporaryTableSql);
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypesNotNull() {
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");

        DwsGaussSqlGenerator sqlGenerator = mock(DwsGaussSqlGenerator.class);
        when(sqlGenerator.buildColumnType(column)).thenCallRealMethod();
        String result = sqlGenerator.buildColumnType(column);

        assertEquals("VARCHAR", result);
    }
}
