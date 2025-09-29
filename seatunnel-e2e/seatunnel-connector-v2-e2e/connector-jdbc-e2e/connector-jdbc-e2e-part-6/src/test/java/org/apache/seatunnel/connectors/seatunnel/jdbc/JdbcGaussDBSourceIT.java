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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.gaussdb.GaussDBCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Disabled("disable because have no gaussdb(dws) docker image")
public class JdbcGaussDBSourceIT extends TestSuiteBase implements TestResource {

    public static final String DWS_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/huaweicloud/dws/huaweicloud-dws-jdbc/8.2.1.300-200/huaweicloud-dws-jdbc-8.2.1.300-200.jar";

    public static final String DWS_JDBC_URL = "jdbc:gaussdb://116.63.81.12:8000/test";

    private static final String SOURCE_TABLE = "test.example_table_1";

    private static final String SOURCE_DB_NAME = "test";

    private static final String SOURCE_USERNAME = "dwsadmin";

    private static final String SOURCE_PASSWORD = "Whaleops1234";

    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    // huaweicloud-ws-jdbc and postgis-jdbc will conflict and cannot be used together
    private static final String PG_JDBC_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-jdbc/2.5.1/postgis-jdbc-2.5.1.jar";
    private static final String PG_GEOMETRY_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-geometry/2.5.1/postgis-geometry-2.5.1.jar";

    public static final String PG_JDBC_URL =
            "jdbc:postgresql://localhost:5432/test?loggerLevel=OFF";

    private static final String SINK_TABLE = "test.example_table_1";

    private static final String SINK_DB_NAME = "test";

    private static final String SINK_USERNAME = "postgres";

    private static final String SINK_PASSWORD = "postgres";

    private Connection sourceConn;
    private Connection sinkConn;

    private static final String CREATE_SOURCE_TABLE_SQL =
            "CREATE TABLE test.example_table_1 (\n"
                    + "col_tinyint int1 NULL,\n"
                    + "col_tinyint_array _int1 NULL,\n"
                    + "col_smallint int2 NULL,\n"
                    + "col_smallint_array _int2 NULL,\n"
                    + "col_integer int4 NULL,\n"
                    + "col_integer_array _int4 NULL,\n"
                    + "col_bigint int8 NULL,\n"
                    + "col_bigint_array _int8 NULL,\n"
                    + "col_numeric numeric(10,2) NULL,\n"
                    + "col_decimal numeric(10,2) NULL,\n"
                    + "col_real float4 NULL,\n"
                    + "col_real_array _float4 NULL,\n"
                    + "col_float4 float4 NULL,\n"
                    + "col_float4_array _float4 NULL,\n"
                    + "col_double_precision float8 NULL,\n"
                    + "col_double_precision_array _float8 NULL,\n"
                    + "col_float8 float8 NULL,\n"
                    + "col_float8_array _float8 NULL,\n"
                    + "col_dec numeric NULL,\n"
                    + "col_money money NULL,\n"
                    + "col_boolean bool NULL,\n"
                    + "col_boolean_array _bool NULL,\n"
                    + "col_char bpchar(10) NULL,\n"
                    + "col_char_array _bpchar NULL,\n"
                    + "col_character bpchar(10) NULL,\n"
                    + "col_character_array _bpchar NULL,\n"
                    + "col_nchar bpchar(10) NULL,\n"
                    + "col_nchar_array _bpchar NULL,\n"
                    + "col_varchar varchar(255) NULL,\n"
                    + "col_varchar_array _varchar NULL,\n"
                    + "col_character_varying varchar(255) NULL,\n"
                    + "col_character_varying_array _varchar NULL,\n"
                    + "col_nvarchar2 nvarchar2(255) NULL,\n"
                    + "col_nvarchar2_array _nvarchar2(255) NULL,\n"
                    + "col_text text NULL,\n"
                    + "col_text_array _text NULL,\n"
                    + "col_date timestamp(0) NULL,\n"
                    + "col_timestamp timestamp NULL,\n"
                    + "col_smalldatetime smalldatetime NULL,\n"
                    + "col_uuid uuid NULL,\n"
                    + "col_json json NULL,\n"
                    + "col_jsonb jsonb NULL,\n"
                    + "col_xml xml NULL,\n"
                    + "PRIMARY KEY (col_integer, col_bigint)\n"
                    + ")\n"
                    + "WITH (\n"
                    + "orientation=row,\n"
                    + "compression=no\n"
                    + ")\n"
                    + "DISTRIBUTE BY HASH(col_integer);";

    private static final String INSERT_SOURCE_DATA_SQL =
            "INSERT INTO test.example_table_1 (\n"
                    + "col_tinyint,\n"
                    + "col_tinyint_array,\n"
                    + "col_smallint,\n"
                    + "col_smallint_array,\n"
                    + "col_integer,\n"
                    + "col_integer_array,\n"
                    + "col_bigint,\n"
                    + "col_bigint_array,\n"
                    + "col_numeric,\n"
                    + "col_decimal,\n"
                    + "col_real,\n"
                    + "col_real_array,\n"
                    + "col_float4,\n"
                    + "col_float4_array,\n"
                    + "col_double_precision,\n"
                    + "col_double_precision_array,\n"
                    + "col_float8,\n"
                    + "col_float8_array,\n"
                    + "col_dec,\n"
                    + "col_money,\n"
                    + "col_boolean,\n"
                    + "col_boolean_array,\n"
                    + "col_char,\n"
                    + "col_char_array,\n"
                    + "col_character,\n"
                    + "col_character_array,\n"
                    + "col_nchar,\n"
                    + "col_nchar_array,\n"
                    + "col_varchar,\n"
                    + "col_varchar_array,\n"
                    + "col_character_varying,\n"
                    + "col_character_varying_array,\n"
                    + "col_nvarchar2,\n"
                    + "col_nvarchar2_array,\n"
                    + "col_text,\n"
                    + "col_text_array,\n"
                    + "col_date,\n"
                    + "col_timestamp,\n"
                    + "col_smalldatetime,\n"
                    + "col_uuid,\n"
                    + "col_json,\n"
                    + "col_jsonb,\n"
                    + "col_xml\n"
                    + ") VALUES (\n"
                    + "3,\n"
                    + "'{1,2,3}',\n"
                    + "10,\n"
                    + "'{10,20,30}',\n"
                    + "103,\n"
                    + "'{100,200,300}',\n"
                    + "1003,\n"
                    + "'{1000,2000,3000}',\n"
                    + "123.45,\n"
                    + "678.90,\n"
                    + "1.23,\n"
                    + "'{1.23, 4.56}',\n"
                    + "3.14,\n"
                    + "'{3.14, 6.28}',\n"
                    + "1234.5678,\n"
                    + "'{1234.5678, 5678.1234}',\n"
                    + "12345.6789,\n"
                    + "'{12345.6789, 6789.12345}',\n"
                    + "123,\n"
                    + "100.50,\n"
                    + "true,\n"
                    + "'{true, false, true}',\n"
                    + "'A',\n"
                    + "'{\"A\",\"B\",\"C\"}',\n"
                    + "'ABC',\n"
                    + "'{\"A\",\"B\",\"C\"}',\n"
                    + "'ABC',\n"
                    + "'{\"A\",\"B\",\"C\"}',\n"
                    + "'ABC',\n"
                    + "'{\"A\",\"B\",\"C\"}',\n"
                    + "'ABC',\n"
                    + "'{\"A\",\"B\",\"C\"}',\n"
                    + "'Hello World',\n"
                    + "'{\"Hello\",\"World\"}',\n"
                    + "'Hello World',\n"
                    + "'{\"Hello\",\"World\"}',\n"
                    + "'2024-02-19',\n"
                    + "'2024-02-19 12:00:00',\n"
                    + "'2024-02-19 12:00:00',\n"
                    + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',\n"
                    + "'{\"key\": \"value\"}',\n"
                    + "'{\"key\": \"value\"}',\n"
                    + "'<root><element>data</element></root>'\n"
                    + ");";

    private final String COLUMN_STRING =
            "col_tinyint, col_tinyint_array, col_smallint, col_smallint_array, "
                    + "col_integer, col_integer_array, col_bigint, col_bigint_array, "
                    + "col_numeric, col_decimal, col_real, col_real_array, "
                    + "col_float4, col_float4_array, col_double_precision, col_double_precision_array, "
                    + "col_float8, col_float8_array, col_dec, col_money, "
                    + "col_boolean, col_boolean_array, col_char, col_char_array, "
                    + "col_character, col_character_array, col_nchar, col_nchar_array, "
                    + "col_varchar, col_varchar_array, col_character_varying, col_character_varying_array, "
                    + "col_nvarchar2, col_nvarchar2_array, col_text, col_text_array, "
                    + "col_date, col_timestamp, col_smalldatetime, col_uuid, "
                    + "col_json, col_jsonb, col_xml";

    private Connection getDwsJdbcConnection() throws SQLException {
        return DriverManager.getConnection(DWS_JDBC_URL, SOURCE_USERNAME, SOURCE_PASSWORD);
    }

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(PG_JDBC_URL, SINK_USERNAME, SINK_PASSWORD);
    }

    private void initializeDwsJdbcTable() {
        try {
            sourceConn = getDwsJdbcConnection();
            try (Statement statement = sourceConn.createStatement()) {
                // create test databases
                statement.execute(String.format("drop table if exists %s", SOURCE_TABLE));
                statement.execute(CREATE_SOURCE_TABLE_SQL);
                statement.execute(INSERT_SOURCE_DATA_SQL);
                log.info("create source table succeed");
            } catch (SQLException e) {
                throw new RuntimeException("Initializing table failed!", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Initializing jdbc failed!", e);
        }
    }

    private void initializePgJdbc() {
        try {
            sinkConn = getPgJdbcConnection();
        } catch (Exception e) {
            throw new RuntimeException("Initializing jdbc failed!", e);
        }
    }

    private SeaTunnelRow genDorisTestData() {
        SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            3, // col_tinyint
                            (new Object[] {1, 2, 3}).toString(), // col_tinyint_array
                            10, // col_smallint
                            (new Object[] {10, 20, 30}).toString(), // col_smallint_array
                            103, // col_integer
                            (new Object[] {100, 200, 300}).toString(), // col_integer_array
                            1003L, // col_bigint
                            (new Object[] {1000L, 2000L, 3000L}).toString(), // col_bigint_array
                            new BigDecimal("123.45"), // col_numeric
                            new BigDecimal("678.90"), // col_decimal
                            1.23f, // col_real
                            (new Object[] {1.23f, 4.56f}).toString(), // col_real_array
                            3.14f, // col_float4
                            (new Object[] {3.14f, 6.28f}).toString(), // col_float4_array
                            1234.5678, // col_double_precision
                            (new Object[] {1234.5678, 5678.1234})
                                    .toString(), // col_double_precision_array
                            12345.6789, // col_float8
                            (new Object[] {12345.6789, 6789.12345}).toString(), // col_float8_array
                            123, // col_dec
                            100.50, // col_money
                            true, // col_boolean
                            (new Object[] {true, false, true}).toString(), // col_boolean_array
                            "A", // col_char
                            (new String[] {"A", "B", "C"}).toString(), // col_char_array
                            "ABC", // col_character
                            (new String[] {"A", "B", "C"}).toString(), // col_character_array
                            "ABC", // col_nchar
                            (new String[] {"A", "B", "C"}).toString(), // col_nchar_array
                            "ABC", // col_varchar
                            (new String[] {"A", "B", "C"}).toString(), // col_varchar_array
                            "ABC", // col_character_varying
                            (new String[] {"A", "B", "C"})
                                    .toString(), // col_character_varying_array
                            "ABC", // col_nvarchar2
                            (new String[] {"A", "B", "C"}).toString(), // col_nvarchar2_array
                            "Hello World", // col_text
                            (new String[] {"Hello", "World"}).toString(), // col_text_array
                            "2024-02-19", // col_date
                            "2024-02-19 12:00:00", // col_timestamp
                            "2024-02-19 12:00:00", // col_smalldatetime
                            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12", // col_uuid
                            "{\"key\": \"value\"}", // col_json
                            "{\"key\": \"value\"}", // col_jsonb
                            "<root><element>data</element></root>" // col_xml
                        });
        log.info("generate test data succeed");
        return seaTunnelRow;
    }

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/connector-jdbc && cd /tmp/seatunnel/plugins/connector-jdbc && wget "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());

                // download DWS_DRIVER_JAR is slow so copy the jar from local
                Path schemaPath =
                        new File("/Users/gaojun/Downloads/dws_8.1.x_jdbc_driver/jdbc/gsjdbc200.jar")
                                .toPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(schemaPath),
                        "/tmp/seatunnel/plugins/connector-jdbc/gsjdbc200.jar");

                //                extraCommands =
                //                        container.execInContainer(
                //                                "bash",
                //                                "-c",
                //                                "mkdir -p /tmp/seatunnel/plugins/jdbc/lib && cd
                // /tmp/seatunnel/plugins/jdbc/lib && wget "
                //                                        + DWS_DRIVER_JAR);
                //                Assertions.assertEquals(0, extraCommands.getExitCode(),
                // extraCommands.getStderr());
            };

    @TestTemplate
    public void test(TestContainer container) throws IOException, InterruptedException {
        initializeDwsJdbcTable();
        initializePgJdbc();

        Container.ExecResult execResult = container.executeJob("/jdbc_dws_to_pg.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        checkSinkDataType();
        checkSinkData();
    }

    private void checkSinkDataType() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("col_tinyint", "SMALLINT");
        map.put("col_tinyint_array", "ARRAY<SMALLINT>");
        map.put("col_smallint", "SMALLINT");
        map.put("col_smallint_array", "ARRAY<SMALLINT>");
        map.put("col_integer", "INT");
        map.put("col_integer_array", "ARRAY<INT>");
        map.put("col_bigint", "BIGINT");
        map.put("col_bigint_array", "ARRAY<BIGINT>");
        map.put("col_numeric", "Decimal(10, 2)");
        map.put("col_decimal", "Decimal(10, 2)");
        map.put("col_real", "FLOAT");
        map.put("col_real_array", "ARRAY<FLOAT>");
        map.put("col_float4", "FLOAT");
        map.put("col_float4_array", "ARRAY<FLOAT>");
        map.put("col_double_precision", "DOUBLE");
        map.put("col_double_precision_array", "ARRAY<DOUBLE>");
        map.put("col_float8", "DOUBLE");
        map.put("col_float8_array", "ARRAY<DOUBLE>");
        map.put("col_dec", "Decimal(38, 10)");
        map.put("col_money", "Decimal(30, 2)");
        map.put("col_boolean", "BOOLEAN");
        map.put("col_boolean_array", "ARRAY<BOOLEAN>");
        map.put("col_char", "STRING");
        map.put("col_char_array", "ARRAY<STRING>");
        map.put("col_character", "STRING");
        map.put("col_character_array", "ARRAY<STRING>");
        map.put("col_nchar", "STRING");
        map.put("col_nchar_array", "ARRAY<STRING>");
        map.put("col_varchar", "STRING");
        map.put("col_varchar_array", "ARRAY<STRING>");
        map.put("col_character_varying", "STRING");
        map.put("col_character_varying_array", "ARRAY<STRING>");
        map.put("col_nvarchar2", "STRING");
        map.put("col_nvarchar2_array", "ARRAY<STRING>");
        map.put("col_text", "STRING");
        map.put("col_text_array", "ARRAY<STRING>");
        map.put("col_date", "TIMESTAMP");
        map.put("col_timestamp", "TIMESTAMP");
        map.put("col_smalldatetime", "TIMESTAMP");
        map.put("col_uuid", "STRING");
        map.put("col_json", "STRING");
        map.put("col_jsonb", "STRING");
        map.put("col_xml", "STRING");

        GaussDBCatalogFactory gaussDBCatalogFactory = new GaussDBCatalogFactory();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("base-url", DWS_JDBC_URL);
        configMap.put("username", SOURCE_USERNAME);
        configMap.put("password", SOURCE_PASSWORD);
        configMap.put("schema", "test");
        Catalog catalog =
                gaussDBCatalogFactory.createCatalog(
                        DatabaseIdentifier.GAUSSDB, ReadonlyConfig.fromMap(configMap));
        CatalogTable table =
                catalog.getTable(
                        TablePath.of(String.format("%s.%s", SOURCE_DB_NAME, SOURCE_TABLE)));
        table.getTableSchema()
                .getColumns()
                .forEach(
                        column -> {
                            Assertions.assertTrue(
                                    column.getDataType()
                                            .toString()
                                            .equalsIgnoreCase(map.get(column.getName())));
                        });
    }

    private void checkSinkData() {
        try {
            assertHasData(SOURCE_DB_NAME, SOURCE_TABLE);

            String sourceSql =
                    String.format(
                            "select * from %s.%s order by col_integer",
                            SOURCE_DB_NAME, SOURCE_TABLE);
            String sinkSql =
                    String.format(
                            "select * from %s.%s order by col_integer", SINK_DB_NAME, SINK_TABLE);
            List<String> columnList =
                    Arrays.stream(COLUMN_STRING.split(","))
                            .map(x -> x.trim())
                            .collect(Collectors.toList());
            Statement sourceStatement =
                    sourceConn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement sinkStatement =
                    sinkConn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            if (!sourceValue.equalsIgnoreCase(sinkValue)) {
                                if (column.equalsIgnoreCase("col_dec")) {
                                    Assertions.assertEquals("123", sourceValue);
                                    Assertions.assertEquals("123.0000000000", sinkValue);
                                } else if (column.equalsIgnoreCase("col_real_array")) {
                                    Assertions.assertEquals("{1.23000002,4.55999994}", sourceValue);
                                    Assertions.assertEquals("{1.23,4.56}", sinkValue);
                                } else if (column.equalsIgnoreCase("col_float4_array")) {
                                    Assertions.assertEquals("{3.1400001,6.28000021}", sourceValue);
                                    Assertions.assertEquals("{3.14,6.28}", sinkValue);
                                } else if (column.equalsIgnoreCase("col_double_precision_array")) {
                                    Assertions.assertEquals(
                                            "{1234.56780000000003,5678.1234000000004}",
                                            sourceValue);
                                    Assertions.assertEquals("{1234.5678,5678.1234}", sinkValue);
                                } else if (column.equalsIgnoreCase("col_float8_array")) {
                                    Assertions.assertEquals(
                                            "{12345.6789000000008,6789.12345000000005}",
                                            sourceValue);
                                    Assertions.assertEquals("{12345.6789,6789.12345}", sinkValue);
                                } else {
                                    Assertions.assertEquals(sourceValue, sinkValue);
                                }
                            }
                        }
                    }
                }
            }
            // Check the row numbers is equal
            sourceResultSet.last();
            sinkResultSet.last();
            Assertions.assertEquals(sourceResultSet.getRow(), sinkResultSet.getRow());
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    private void assertHasData(String db, String table) {
        try (Statement statement = sourceConn.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", db, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }
}
