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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcBatchStatementExecutorBuilderTest {

    @Test
    void replacingMergeTreeUsesQueryUpsertWhenSupportUpsertEnabled() throws Exception {
        JdbcBatchStatementExecutor executor = newBaseBuilder().setSupportUpsert(true).build();

        assertInstanceOf(ReduceBufferedBatchStatementExecutor.class, executor);
        JdbcBatchStatementExecutor innerExecutor =
                getField(executor, "insertOrUpdateExecutor", JdbcBatchStatementExecutor.class);

        assertInstanceOf(InsertOrUpdateBatchStatementExecutor.class, innerExecutor);
        assertFalse(getField(executor, "ignoreUpdateBefore", Boolean.class));
    }

    @Test
    void replacingMergeTreeKeepsInsertOnlyModeWhenSupportUpsertDisabled() throws Exception {
        JdbcBatchStatementExecutor executor = newBaseBuilder().setSupportUpsert(false).build();

        assertInstanceOf(ReduceBufferedBatchStatementExecutor.class, executor);
        JdbcBatchStatementExecutor innerExecutor =
                getField(executor, "insertOrUpdateExecutor", JdbcBatchStatementExecutor.class);

        assertInstanceOf(SimpleBatchStatementExecutor.class, innerExecutor);
        assertTrue(getField(executor, "ignoreUpdateBefore", Boolean.class));
    }

    private JdbcBatchStatementExecutorBuilder newBaseBuilder() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"user_id", "email"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        Map<String, String> clickhouseTableSchema = new LinkedHashMap<>();
        clickhouseTableSchema.put("user_id", "Int32");
        clickhouseTableSchema.put("email", "String");

        return new JdbcBatchStatementExecutorBuilder()
                .setTable("users1")
                .setTableEngine("ReplacingMergeTree")
                .setRowType(rowType)
                .setPrimaryKeys(new String[] {"user_id"})
                .setOrderByKeys(new String[] {"user_id"})
                .setClickhouseTableSchema(clickhouseTableSchema);
    }

    private <T> T getField(Object target, String fieldName, Class<T> fieldType) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return fieldType.cast(field.get(target));
    }
}
