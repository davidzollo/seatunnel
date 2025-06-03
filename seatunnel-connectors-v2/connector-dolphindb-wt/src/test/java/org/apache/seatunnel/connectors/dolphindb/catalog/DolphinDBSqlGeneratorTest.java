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

package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dolphindb.jdbc.Utils;
import com.google.common.collect.Lists;

import java.sql.SQLException;

class DolphinDBSqlGeneratorTest {

    @Test
    void generateDeleteRowSql() throws SQLException {
        String[] fields = Lists.newArrayList("id", "name", "age").toArray(new String[0]);
        BasicType[] seaTunnelRowTypes =
                Lists.newArrayList(BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        .toArray(new BasicType[0]);
        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fields, seaTunnelRowTypes);
        String sql =
                DolphinDBSqlGenerator.generateDeleteRowSql(
                        "dfs://whalescheduler", "users", seaTunnelRowType);
        Assertions.assertEquals("delete from users where id = ? , name = ? , age = ?", sql);

        String tableName = Utils.getTableName(sql, true);
        Assertions.assertEquals("users", tableName);
    }
}
