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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests ClickHouse table identifier quoting used by catalog and SQL helpers. */
class ClickhouseUtilTest {

    @Test
    void quoteTableIdentifierQuotesSpecialTableName() {
        Assertions.assertEquals("`9-1_users1`", ClickhouseUtil.quoteTableIdentifier("9-1_users1"));
    }

    @Test
    void quoteTableIdentifierKeepsAlreadyQuotedFullName() {
        Assertions.assertEquals(
                "`st3355_empty_db`.`9-1_users1`",
                ClickhouseUtil.quoteTableIdentifier("`st3355_empty_db`.`9-1_users1`"));
    }
}
