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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

public class RedshiftCatalogTest {

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
        RedshiftCatalogFactory factory = new RedshiftCatalogFactory();
        RedshiftCatalog catalog =
                (RedshiftCatalog)
                        factory.createCatalog(
                                "test",
                                ReadonlyConfig.fromMap(
                                        new HashMap<String, Object>() {
                                            {
                                                put(
                                                        "base-url",
                                                        "jdbc:redshift://localhost:5432/test");
                                                put("username", "test");
                                                put("password", "test");
                                            }
                                        }));
        String sql = catalog.getCreateTableSql(TablePath.of("test.test.test"), CATALOG_TABLE, true);
        Assertions.assertEquals(
                "CREATE TABLE \"test\".\"test\" (\n"
                        + "\"test\" CHARACTER VARYING(65535),\n"
                        + "\"test2\" CHARACTER VARYING(65535),\n"
                        + "\"test3\" CHARACTER VARYING(65535),\n"
                        + "PRIMARY KEY (\"test\",\"test2\")\n"
                        + ");",
                sql);
    }

    /**
     * Verify that query metadata lookup uses the database from table_path and keeps the
     * Redshift-specific type mapper instead of falling back to AbstractJdbcCatalog defaults.
     */
    @Test
    void testQueryMetadataLookupUsesTargetDatabaseAndRedshiftMapper() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        AtomicReference<String> capturedUrl = new AtomicReference<>();
        RedshiftCatalog catalog =
                new RedshiftCatalog(
                        "test",
                        "test",
                        "test",
                        new org.apache.seatunnel.common.utils.JdbcUrlUtil.UrlInfo(
                                "jdbc:redshift://localhost:5432/default_db",
                                "jdbc:redshift://localhost:5432",
                                "localhost",
                                5432,
                                "default_db",
                                ""),
                        "public") {
                    @Override
                    protected Connection getConnection(String url) {
                        capturedUrl.set(url);
                        return connection;
                    }
                };
        TablePath tablePath = TablePath.of("qa_sink", "public", "orders");
        String sqlQuery = "SELECT id FROM public.orders";

        try (MockedStatic<CatalogUtils> catalogUtils = Mockito.mockStatic(CatalogUtils.class)) {
            catalogUtils
                    .when(
                            () ->
                                    CatalogUtils.getCatalogTable(
                                            Mockito.same(connection),
                                            Mockito.eq(sqlQuery),
                                            Mockito.argThat(
                                                    mapper ->
                                                            mapper instanceof RedshiftTypeMapper)))
                    .thenReturn(CATALOG_TABLE);

            CatalogTable actual = catalog.getTable(tablePath, sqlQuery);

            Assertions.assertSame(CATALOG_TABLE, actual);
            Assertions.assertEquals("jdbc:redshift://localhost:5432/qa_sink", capturedUrl.get());
        }
    }
}
