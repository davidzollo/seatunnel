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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefineSinkTypeTransformTest {

    @Test
    void transformRowReturnsInputRow() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "default"),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                PhysicalColumn.of(
                                                        "col1",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null),
                                                PhysicalColumn.of(
                                                        "col2",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null)))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        "test_catalog");

        DefineSinkTypeTransformConfig config =
                new DefineSinkTypeTransformConfig(Collections.emptyList());
        DefineSinkTypeTransform transform = new DefineSinkTypeTransform(config, catalogTable);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"value1", "value2"});
        SeaTunnelRow resultRow = transform.transformRow(inputRow);
        assertEquals(inputRow, resultRow);
    }

    @Test
    void transformTableSchemaUpdatesColumnTypes() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "default"),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                PhysicalColumn.of(
                                                        "col1",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null),
                                                PhysicalColumn.of(
                                                        "col2",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null)))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        "test_catalog");

        DefineSinkTypeTransformConfig config =
                new DefineSinkTypeTransformConfig(
                        Arrays.asList(
                                new DefineSinkTypeTransformConfig.DefineColumnType(
                                        "col1", "varchar(10)"),
                                new DefineSinkTypeTransformConfig.DefineColumnType(
                                        "col2", "integer")));
        DefineSinkTypeTransform transform = new DefineSinkTypeTransform(config, catalogTable);
        TableSchema resultSchema = transform.transformTableSchema();
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"value1", "value2"});
        SeaTunnelRow resultRow = transform.transformRow(inputRow);
        assertEquals(inputRow, resultRow);

        assertEquals("varchar(10)", resultSchema.getColumns().get(0).getSinkType());
        assertEquals("integer", resultSchema.getColumns().get(1).getSinkType());
    }

    @Test
    void constructorThrowsExceptionForInvalidColumn() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "default"),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                PhysicalColumn.of(
                                                        "col1",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null),
                                                PhysicalColumn.of(
                                                        "col2",
                                                        BasicType.STRING_TYPE,
                                                        (Integer) null,
                                                        false,
                                                        null,
                                                        null)))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        "test_catalog");

        DefineSinkTypeTransformConfig config =
                new DefineSinkTypeTransformConfig(
                        Arrays.asList(
                                new DefineSinkTypeTransformConfig.DefineColumnType(
                                        "invalid_col", "varchar(10)")));

        assertThrows(
                IllegalArgumentException.class,
                () -> new DefineSinkTypeTransform(config, catalogTable));
    }
}
