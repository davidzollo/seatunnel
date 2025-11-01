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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for FieldNamedPreparedStatement class */
public class FieldNamedPreparedStatementTest {

    @Mock private Connection mockConnection;

    @Mock private PreparedStatement mockStatement;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    /** Test prepareStatement with null parameters should throw IllegalArgumentException */
    @Test
    void testPrepareStatementWithNullConnection() {
        String sql = "INSERT INTO test (name, age) VALUES (?, ?)";
        String[] fieldNames = {"name", "age"};

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () -> FieldNamedPreparedStatement.prepareStatement(null, sql, fieldNames));

        assertTrue(exception.getMessage().contains("connection must not be null"));
    }

    /** Test prepareStatement with null SQL should throw IllegalArgumentException */
    @Test
    void testPrepareStatementWithNullSql() {
        String[] fieldNames = {"name", "age"};

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, null, fieldNames));

        assertTrue(exception.getMessage().contains("sql must not be null"));
    }

    /** Test prepareStatement with null fieldNames should throw IllegalArgumentException */
    @Test
    void testPrepareStatementWithNullFieldNames() {
        String sql = "INSERT INTO test (name, age) VALUES (?, ?)";

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, sql, null));

        assertTrue(exception.getMessage().contains("fieldNames must not be null"));
    }

    /** Test prepareStatement with standard ? placeholders */
    @Test
    void testPrepareStatementWithStandardPlaceholders() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (?, ?)";
        String[] fieldNames = {"name", "age"};

        when(mockConnection.prepareStatement(sql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(sql);
    }

    /** Test prepareStatement with named parameters */
    @Test
    void testPrepareStatementWithNamedParameters() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (:name, :age)";
        String[] fieldNames = {"name", "age"};
        String expectedSql = "INSERT INTO test (name, age) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test prepareStatement with repeated named parameters */
    @Test
    void testPrepareStatementWithRepeatedNamedParameters() throws SQLException {
        String sql = "INSERT INTO test (name, email, name2) VALUES (:name, :email, :name)";
        String[] fieldNames = {"name", "email"};
        String expectedSql = "INSERT INTO test (name, email, name2) VALUES (?, ?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test prepareStatement with parameter names containing underscores and numbers */
    @Test
    void testPrepareStatementWithComplexParameterNames() throws SQLException {
        String sql =
                "INSERT INTO test (field_1, field2, field_name_3) VALUES (:field_1, :field2, :field_name_3)";
        String[] fieldNames = {"field_1", "field2", "field_name_3"};
        String expectedSql = "INSERT INTO test (field_1, field2, field_name_3) VALUES (?, ?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test prepareStatement with spaces after parameter names */
    @Test
    void testPrepareStatementWithSpacesAfterParameterNames() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (:name , :age )";
        String[] fieldNames = {"name", "age"};
        String expectedSql = "INSERT INTO test (name, age) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /**
     * Test prepareStatement with mismatched parameter count should throw IllegalArgumentException
     */
    @Test
    void testPrepareStatementWithMismatchedParameterCount() {
        String sql = "INSERT INTO test (name, age, email) VALUES (:name, :age, :email)";
        String[] fieldNames = {"name", "age"}; // Missing email parameter

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, sql, fieldNames));

        // The exception should be thrown due to parameter count mismatch
        assertNotNull(exception);
    }

    /** Test prepareStatement with missing parameter in SQL should throw IllegalArgumentException */
    @Test
    void testPrepareStatementWithMissingParameterInSql() {
        String sql = "INSERT INTO test (name, age) VALUES (:name, :age)";
        String[] fieldNames = {"name", "email"}; // email parameter not in SQL

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, sql, fieldNames));

        assertTrue(
                exception.getMessage().contains("email")
                        && exception.getMessage().contains("doesn't exist in the parameters"));
    }

    /** Test prepareStatement with empty parameter name should throw IllegalArgumentException */
    @Test
    void testPrepareStatementWithEmptyParameterName() {
        String sql = "INSERT INTO test (name, age) VALUES (:, :age)";
        String[] fieldNames = {"name", "age"};

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, sql, fieldNames));

        assertTrue(
                exception
                        .getMessage()
                        .contains("Named parameters in SQL statement must not be empty"));
    }

    /** Test parameter binding functionality through delegation */
    @Test
    void testParameterBinding() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (?, ?)";
        String[] fieldNames = {"name", "age"};

        when(mockConnection.prepareStatement(sql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        // Test string parameter binding
        statement.setString(1, "John");
        verify(mockStatement).setString(1, "John");

        // Test int parameter binding
        statement.setInt(2, 25);
        verify(mockStatement).setInt(2, 25);

        // Test execution delegation
        statement.executeUpdate();
        verify(mockStatement).executeUpdate();

        // Test close delegation
        statement.close();
        verify(mockStatement).close();
    }

    /** Test parameter binding with repeated parameters */
    @Test
    void testParameterBindingWithRepeatedParameters() throws SQLException {
        String sql = "INSERT INTO test (name, email, name2) VALUES (:name, :email, :name)";
        String[] fieldNames = {"name", "email"};
        String expectedSql = "INSERT INTO test (name, email, name2) VALUES (?, ?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        // Setting parameter 1 (name) should bind to positions 1 and 3
        statement.setString(1, "John");
        verify(mockStatement).setString(1, "John");
        verify(mockStatement).setString(3, "John");

        // Setting parameter 2 (email) should bind to position 2
        statement.setString(2, "john@example.com");
        verify(mockStatement).setString(2, "john@example.com");
    }

    /** Test SQL with no parameters */
    @Test
    void testPrepareStatementWithNoParameters() throws SQLException {
        String sql = "SELECT * FROM test";
        String[] fieldNames = {};

        when(mockConnection.prepareStatement(sql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(sql);
    }

    /** Test parameter names with leading spaces */
    @Test
    void testPrepareStatementWithLeadingSpacesInParameterName() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (:  name, :   age)";
        String[] fieldNames = {"name", "age"};
        String expectedSql = "INSERT INTO test (name, age) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test parameter names with trailing spaces */
    @Test
    void testPrepareStatementWithTrailingSpacesInParameterName() throws SQLException {
        String sql = "INSERT INTO test (name, age) VALUES (:name  , :age   )";
        String[] fieldNames = {"name", "age"};
        String expectedSql = "INSERT INTO test (name, age) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test parameter names with spaces in the middle */
    @Test
    void testPrepareStatementWithSpacesInMiddleOfParameterName() throws SQLException {
        String sql = "INSERT INTO test (first_name, last_name) VALUES (:first name, :last name)";
        String[] fieldNames = {"first name", "last name"};
        String expectedSql = "INSERT INTO test (first_name, last_name) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test parameter names with spaces before, middle, and after */
    @Test
    void testPrepareStatementWithSpacesEverywhereInParameterName() throws SQLException {
        String sql = "INSERT INTO test (full_name) VALUES (:  full name  )";
        String[] fieldNames = {"full name"};
        String expectedSql = "INSERT INTO test (full_name) VALUES (?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement result =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(expectedSql);
    }

    /** Test parameter binding with spaced parameter names */
    @Test
    void testParameterBindingWithSpacedParameterNames() throws SQLException {
        String sql = "INSERT INTO test (first_name, last_name) VALUES (:first name, :last name)";
        String[] fieldNames = {"first name", "last name"};
        String expectedSql = "INSERT INTO test (first_name, last_name) VALUES (?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        // Test parameter binding with spaced names
        statement.setString(1, "John");
        verify(mockStatement).setString(1, "John");

        statement.setString(2, "Doe");
        verify(mockStatement).setString(2, "Doe");
    }

    /** Test repeated parameter names with spaces */
    @Test
    void testRepeatedParameterNamesWithSpaces() throws SQLException {
        String sql =
                "INSERT INTO log (message, user, message_backup) VALUES (:user message, :user, :user message)";
        String[] fieldNames = {"user message", "user"};
        String expectedSql = "INSERT INTO log (message, user, message_backup) VALUES (?, ?, ?)";

        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockStatement);

        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(mockConnection, sql, fieldNames);

        // Setting parameter 1 (user message) should bind to positions 1 and 3
        statement.setString(1, "Hello World");
        verify(mockStatement).setString(1, "Hello World");
        verify(mockStatement).setString(3, "Hello World");

        // Setting parameter 2 (user) should bind to position 2
        statement.setString(2, "john");
        verify(mockStatement).setString(2, "john");
    }

    /** Test parameter name that becomes empty after trimming spaces */
    @Test
    void testParameterNameWithOnlySpaces() {
        String sql = "INSERT INTO test (name) VALUES (:   )";
        String[] fieldNames = {"   "};

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                FieldNamedPreparedStatement.prepareStatement(
                                        mockConnection, sql, fieldNames));

        assertTrue(
                exception
                        .getMessage()
                        .contains("Named parameters in SQL statement must not be empty"));
    }
}
