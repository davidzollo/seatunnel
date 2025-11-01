/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.informix;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

@Disabled("Temporarily disabled - needs to be fixed")
public class InformixCatalogTest {

    private static final CatalogTable CATALOG_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .columns(
                                    Arrays.asList(
                                            PhysicalColumn.of(
                                                    "test",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "test2",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "test3",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    "")))
                            .primaryKey(
                                    new PrimaryKey(
                                            "test_primary_keys", Arrays.asList("test", "test2")))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    void testCreateTableSqlWithPrimaryKeys() {
        InformixCatalogFactory factory = new InformixCatalogFactory();
        InformixCatalog catalog =
                (InformixCatalog)
                        factory.createCatalog(
                                "test",
                                ReadonlyConfig.fromMap(
                                        new HashMap<String, Object>() {
                                            {
                                                put(
                                                        "base-url",
                                                        "jdbc:kingbase://localhost:5432/test");
                                                put("username", "test");
                                                put("password", "test");
                                            }
                                        }));
        String sql = catalog.getCreateTableSql(TablePath.of("test.test.test"), CATALOG_TABLE);
        Assertions.assertEquals(
                "CREATE TABLE IF NOT EXISTS test.test (\n"
                        + "test TEXT,\n"
                        + "test2 TEXT,\n"
                        + "test3 TEXT,\n"
                        + "PRIMARY KEY (test, test2)\n"
                        + ");",
                sql);
    }

    @Test
    public void testInformixWithDatabase() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo(
                        "jdbc:informix-sqli://localhost:9088/sysmaster:INFORMIXSERVER=informix;NEWCODESET=gb18030,8859-1,819;DB_LOCALE=en_US.819;ifx_use_strenc=true",
                        InformixCatalogFactory.URL_PATTERN);
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("sysmaster", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals(
                "jdbc:informix-sqli://localhost:9088/sysmaster:INFORMIXSERVER=informix;NEWCODESET=gb18030,8859-1,819;DB_LOCALE=en_US.819;ifx_use_strenc=true",
                urlInfo.getUrlWithDatabase().get());
        Assertions.assertEquals(
                "jdbc:informix-sqli://localhost:9088", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(9088, urlInfo.getPort());
    }
}
