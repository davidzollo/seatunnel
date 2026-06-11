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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests generated ClickHouse sink SQL for quoted table and column identifiers. */
class SqlUtilsTest {

    @Test
    void getInsertIntoStatementQuotesClickHouseTableIdentifier() {
        String statement =
                SqlUtils.getInsertIntoStatement("9-1_users1", new String[] {"user_id", "desc_col"});

        Assertions.assertEquals(
                "INSERT INTO `9-1_users1` (`user_id`, `desc_col`) VALUES (:user_id, :desc_col)",
                statement);
    }

    @Test
    void quoteIdentifierEscapesBackticks() {
        Assertions.assertEquals("`desc``col`", SqlUtils.quoteIdentifier("desc`col"));
    }

    @Test
    void getMutationStatementsQuoteClickHouseTableIdentifier() {
        String[] conditionFields = {"user_id"};

        Assertions.assertEquals(
                "DELETE FROM `9-1_users1` WHERE `user_id` = :user_id",
                SqlUtils.getDeleteStatement("9-1_users1", conditionFields, false));
        Assertions.assertEquals(
                "ALTER TABLE `9-1_users1` DELETE WHERE `user_id` = :user_id settings mutations_sync = 1",
                SqlUtils.getAlterTableDeleteStatement("9-1_users1", conditionFields));
        Assertions.assertEquals(
                "SELECT 1 FROM `9-1_users1` WHERE `user_id` = :user_id",
                SqlUtils.getRowExistsStatement("9-1_users1", conditionFields));
    }
}
