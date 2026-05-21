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

package io.debezium.connector.kingbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.kingbase8.core.TypeInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class TypeRegistryTest {

    @Test
    public void testNormalizeTypeNameHandlesUppercaseJdbcMetadataNames() {
        Assertions.assertEquals("int4", TypeRegistry.normalizeTypeName("INT4"));
        Assertions.assertEquals("varchar", TypeRegistry.normalizeTypeName("VARCHAR(100)"));
        Assertions.assertEquals("timestamp", TypeRegistry.normalizeTypeName("public.TIMESTAMP"));
    }

    @Test
    public void testNormalizeTypeNamePreservesArrayLookupSemantics() {
        Assertions.assertEquals("_int4", TypeRegistry.normalizeTypeName("INTEGER[]"));
        Assertions.assertEquals("_varchar", TypeRegistry.normalizeTypeName("_VARCHAR"));
        Assertions.assertEquals(
                "_timestamp", TypeRegistry.normalizeTypeName("TIMESTAMP WITHOUT TIME ZONE[]"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSqlTypeMapperCoreTypeCacheIsCaseInsensitive() throws Exception {
        Class<?> mapperClass =
                Class.forName("io.debezium.connector.kingbase.TypeRegistry$SqlTypeMapper");
        Method collectTypeNames = mapperClass.getDeclaredMethod("collectTypeNames", Iterator.class);
        collectTypeNames.setAccessible(true);

        Set<String> collected =
                (Set<String>)
                        collectTypeNames.invoke(null, Arrays.asList("INT4", "VARCHAR").iterator());

        Assertions.assertTrue(collected.contains("int4"));
        Assertions.assertTrue(collected.contains("varchar"));
    }

    @Test
    public void testSqlTypeMapperUsesDriverCoreTypeCacheWithOriginalCase() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        TypeInfo typeInfo = Mockito.mock(TypeInfo.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(false);
        Mockito.when(typeInfo.getKBTypeNamesWithSQLTypes())
                .thenReturn(Arrays.asList("INT4").iterator())
                .thenReturn(Arrays.asList("INT4").iterator());
        Mockito.when(typeInfo.getSQLType("INT4")).thenReturn(Types.INTEGER);

        Class<?> mapperClass =
                Class.forName("io.debezium.connector.kingbase.TypeRegistry$SqlTypeMapper");
        Constructor<?> constructor =
                mapperClass.getDeclaredConstructor(Connection.class, TypeInfo.class);
        constructor.setAccessible(true);
        Object mapper = constructor.newInstance(connection, typeInfo);
        Method getSqlType = mapperClass.getDeclaredMethod("getSqlType", String.class);
        getSqlType.setAccessible(true);

        Assertions.assertEquals(Types.INTEGER, getSqlType.invoke(mapper, "int4"));
        Mockito.verify(typeInfo).getSQLType("INT4");
        Mockito.verify(typeInfo, Mockito.never()).getSQLType("int4");
    }
}
