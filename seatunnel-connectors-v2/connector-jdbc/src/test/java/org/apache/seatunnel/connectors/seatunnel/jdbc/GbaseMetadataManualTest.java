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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GbaseMetadataManualTest {
    private static final String DRIVER_CLASS = "com.gbase.jdbc.Driver";

    private static final String URL = "jdbc:gbase://IP:5258/testdb?vcName=vcname000001";

    private static final String USER = "root";
    private static final String PASSWORD = "";

    private static final String QUERY =
            "SELECT * FROM (select * from testdb.table_name) temp where 1 = 0";

    private static final String QUERY_WITHOUT_FIX = "select * from testdb.table_name";

    @Disabled("Manual test only")
    @Test
    public void testWithCustomMemory_InCode() throws Exception {
        TestResult result1 = runInSeparateJVM("128m", "64m", QUERY_WITHOUT_FIX);

        TestResult result2 = runInSeparateJVM("128m", "64m", QUERY);

        if (result1.exitCode != 2) {
            System.err.println();
            System.err.println(
                    "Actual exitCode: "
                            + result1.exitCode
                            + " ("
                            + (result1.exitCode == 1 ? "General Exception" : "Unknown")
                            + ")");
            System.err.println("Error type: " + result1.errorType);
            System.err.println("Full subprocess output:");
            System.err.println(result1.output);
        }
        Assertions.assertEquals(2, result1.exitCode, "Unfixed query should cause OOM");
        Assertions.assertEquals(
                "OutOfMemoryError",
                result1.errorType,
                "Unfixed query should throw OutOfMemoryError");

        Assertions.assertEquals(0, result2.exitCode, "Fixed query should succeed");
        Assertions.assertNull(result2.errorType, "Fixed query should not throw error");
        Assertions.assertTrue(result2.success, "Fixed query should get metadata");
    }

    private static class TestResult {
        final int exitCode;
        final String errorType;
        final boolean success;
        final String output;

        TestResult(int exitCode, String errorType, boolean success, String output) {
            this.exitCode = exitCode;
            this.errorType = errorType;
            this.success = success;
            this.output = output;
        }
    }

    private TestResult runInSeparateJVM(String maxHeap, String initialHeap, String query)
            throws Exception {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-Xmx" + maxHeap);
        command.add("-Xms" + initialHeap);
        command.add("-XX:+HeapDumpOnOutOfMemoryError");
        command.add("-XX:HeapDumpPath=/tmp/gbase-oom-" + System.currentTimeMillis() + ".hprof");
        command.add("-cp");
        command.add(classpath);
        command.add(GbaseMetadataTestRunner.class.getName());
        command.add(URL);
        command.add(USER);
        command.add(PASSWORD);
        command.add(query);

        System.out.println(
                "Launching subprocess with maxHeap=" + maxHeap + ", initialHeap=" + initialHeap);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder outputBuilder = new StringBuilder();
        String errorType = null;
        boolean success = false;

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                outputBuilder.append(line).append("\n");
                System.out.println("  [subprocess] " + line);

                if (line.contains("ERROR_TYPE:")) {
                    errorType =
                            line.substring(line.indexOf("ERROR_TYPE:") + "ERROR_TYPE:".length())
                                    .trim();
                }
                if (line.contains("SUCCESS: true")) {
                    success = true;
                }
            }
        }

        int exitCode = process.waitFor();

        System.out.println("Subprocess exited with code: " + exitCode);
        if (errorType != null) {
            System.out.println("Error type detected: " + errorType);
        }
        System.out.println();

        return new TestResult(exitCode, errorType, success, outputBuilder.toString());
    }

    public static class GbaseMetadataTestRunner {
        public static void main(String[] args) throws Exception {
            if (args.length != 4) {
                System.err.println(
                        "Usage: GbaseMetadataTestRunner <url> <user> <password> <query>");
                System.exit(1);
            }

            String url = args[0];
            String user = args[1];
            String password = args[2];
            String query = args[3];

            System.out.println("=== GbaseMetadataTestRunner Starting ===");
            System.out.println(
                    "JVM Max Memory: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + " MB");
            System.out.println("Query: " + query);

            try {
                System.out.println("Loading driver: " + DRIVER_CLASS);
                Class.forName(DRIVER_CLASS);
                System.out.println("Driver loaded successfully");

                System.out.println("Connecting to database...");
                try (Connection connection = DriverManager.getConnection(url, user, password)) {
                    System.out.println("Connection established");

                    System.out.println("Preparing statement...");
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        System.out.println("Statement prepared");

                        System.out.println("Getting metadata...");
                        ResultSetMetaData metaData = statement.getMetaData();

                        if (metaData != null) {
                            int columnCount = metaData.getColumnCount();
                            System.out.println("Column Count = " + columnCount);
                            System.out.println("SUCCESS: true");
                        } else {
                            System.out.println("Warning: metadata is null");
                        }
                        System.exit(0);
                    }
                }
            } catch (OutOfMemoryError e) {
                System.err.println("ERROR_TYPE: OutOfMemoryError");
                System.err.println("OutOfMemoryError: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(2);
            } catch (SQLException e) {
                System.err.println("ERROR_TYPE: SQLException");
                System.err.println("SQLException: " + e.getMessage());
                System.err.println("SQL State: " + e.getSQLState());
                System.err.println("Error Code: " + e.getErrorCode());
                e.printStackTrace(System.err);
                System.exit(1);
            } catch (ClassNotFoundException e) {
                System.err.println("ERROR_TYPE: ClassNotFoundException");
                System.err.println("ClassNotFoundException: " + e.getMessage());
                System.err.println("Driver class not found: " + DRIVER_CLASS);
                e.printStackTrace(System.err);
                System.exit(1);
            } catch (Exception e) {
                System.err.println("ERROR_TYPE: " + e.getClass().getSimpleName());
                System.err.println("Exception: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(1);
            }
        }
    }
}
