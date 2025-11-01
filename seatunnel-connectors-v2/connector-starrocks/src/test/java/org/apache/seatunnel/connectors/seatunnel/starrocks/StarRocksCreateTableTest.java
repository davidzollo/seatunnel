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

package org.apache.seatunnel.connectors.seatunnel.starrocks;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Disabled("Temporarily disabled - needs to be fixed")
public class StarRocksCreateTableTest {

    @Test
    public void test() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, 100, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, "'N'-N"));
        columns.add(PhysicalColumn.of("gender", BasicType.BYTE_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of("create_time", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "update_time", LocalTimeType.LOCAL_TIME_TYPE, (Long) null, true, null, ""));

        String result =
                StarRocksSaveModeUtil.getCreateTableSql(
                        "CREATE TABLE IF NOT EXISTS `"
                                + SaveModePlaceHolder.DATABASE.getPlaceHolder()
                                + "`.`"
                                + SaveModePlaceHolder.TABLE.getPlaceHolder()
                                + "` (                                                                                                                                                   \n"
                                + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                + "  ,       \n"
                                + "`create_time` DATETIME NOT NULL ,  \n"
                                + SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder()
                                + "  \n"
                                + ") ENGINE=OLAP  \n"
                                + "PRIMARY KEY("
                                + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                + ",`create_time`)  \n"
                                + "PARTITION BY RANGE (`create_time`)(  \n"
                                + "   PARTITION p20230329 VALUES LESS THAN (\"2023-03-29\")                                                                                                                                                           \n"
                                + ")                                      \n"
                                + "DISTRIBUTED BY HASH ("
                                + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                + ")  \n"
                                + "COMMENT \""
                                + SaveModePlaceHolder.COMMENT.getPlaceHolder()
                                + "\"\n"
                                + "PROPERTIES (                           \n"
                                + "    \"dynamic_partition.enable\" = \"true\",                                                                                                                                                                       \n"
                                + "    \"dynamic_partition.time_unit\" = \"DAY\",                                                                                                                                                                     \n"
                                + "    \"dynamic_partition.end\" = \"3\", \n"
                                + "    \"dynamic_partition.prefix\" = \"p\"                                                                                                                                                                           \n"
                                + ");",
                        "test1",
                        "test2",
                        TableSchema.builder()
                                .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                                .columns(columns)
                                .build(),
                        "test comment \\ '1'");

        System.out.println(result);
    }
}
