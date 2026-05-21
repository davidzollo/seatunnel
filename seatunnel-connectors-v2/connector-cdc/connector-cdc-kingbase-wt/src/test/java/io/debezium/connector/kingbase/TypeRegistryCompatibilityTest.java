/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;

/** Covers runtime compatibility for different Kingbase TypeInfo#getSQLType signatures. */
public class TypeRegistryCompatibilityTest {

    @Test
    public void shouldUseObjectBasedSqlTypeSignature() throws SQLException {
        Assertions.assertEquals(
                Types.INTEGER,
                TypeRegistry.invokeGetSqlType(new ObjectSignatureTypeInfo(), "int4"));
    }

    @Test
    public void shouldFallBackToLegacyStringBasedSqlTypeSignature() throws SQLException {
        Assertions.assertEquals(
                Types.VARCHAR,
                TypeRegistry.invokeGetSqlType(new StringSignatureTypeInfo(), "varchar"));
    }

    private static final class ObjectSignatureTypeInfo {

        public int getSQLType(Object typeName) {
            return "int4".equals(typeName) ? Types.INTEGER : Types.OTHER;
        }
    }

    private static final class StringSignatureTypeInfo {

        public int getSQLType(String typeName) {
            return "varchar".equals(typeName) ? Types.VARCHAR : Types.OTHER;
        }
    }
}
