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

package org.apache.seatunnel.transform.fieldrenamer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class RenamerTest {

    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            false,
                                            null,
                                            null,
                                            "int unsigned",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.STRING_TYPE,
                                            10,
                                            false,
                                            null,
                                            null,
                                            "varchar(10)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.STRING_TYPE,
                                            20,
                                            false,
                                            null,
                                            null,
                                            "varchar(20)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    void testConvertName() {
        FieldRenamerConfig config = new FieldRenamerConfig();
        config.setReplacements(new LinkedHashMap<>(Collections.singletonMap("abc", "abc_")));
        FieldRenamerTransform transform =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config);

        Assertions.assertEquals("abc_name", transform.convertName("", "abcname"));
        Assertions.assertEquals("a1b2c3", transform.convertName("", "a1b2c3"));
        Assertions.assertEquals("ddabc_c", transform.convertName("", "ddabcc"));
        Assertions.assertEquals("aBcc", transform.convertName("", "aBcc"));
        Assertions.assertEquals("tabc_abc_1q23", transform.convertName("test2", "tabcabc1q23"));

        FieldRenamerConfig config2 = new FieldRenamerConfig();
        config2.setReplacements(new LinkedHashMap<>(Collections.singletonMap("abc", "")));
        FieldRenamerTransform transform2 =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config2);
        Assertions.assertEquals("1q23", transform2.convertName("", "abc1q23"));

        FieldRenamerConfig config3 = new FieldRenamerConfig();
        config3.setConvertCase(ConvertCase.UPPER);
        config3.setReplacements(new LinkedHashMap<>(Collections.singletonMap("abc", "abc_")));
        config3.setPrefix("abc");
        config3.setSuffix("ee");
        config3.setSpecific(
                Collections.singletonList(
                        new FieldRenamerConfig.SpecificModify("test", "abc1q232", "other")));
        FieldRenamerTransform transform3 =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config3);
        Assertions.assertEquals("abcabc_1Q23ee", transform3.convertName("test", "abc1q23"));
        Assertions.assertEquals("other", transform3.convertName("test", "abc1q232"));

        FieldRenamerConfig config4 = new FieldRenamerConfig();
        config4.setTableMatchRegex("test2");
        config4.setConvertCase(ConvertCase.UPPER);
        config4.setReplacements(new LinkedHashMap<>(Collections.singletonMap("abc", "abc_")));
        config4.setPrefix("abc");
        config4.setSuffix("ee");
        config4.setSpecific(
                Collections.singletonList(
                        new FieldRenamerConfig.SpecificModify("test", "abc1q232", "other")));
        FieldRenamerTransform transform4 =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config4);
        Assertions.assertEquals("abcabc_1Q23ee", transform4.convertName("test2", "abc1q23"));
        Assertions.assertEquals("other", transform4.convertName("test", "abc1q232"));
        Assertions.assertEquals("abc1q23", transform4.convertName("test", "abc1q23"));

        FieldRenamerConfig config5 = new FieldRenamerConfig();
        config5.setTableMatchRegex("test2");
        config5.setConvertCase(ConvertCase.UPPER);
        config5.setPrefix("p_");
        config5.setSuffix("_s");
        LinkedHashMap<String, String> replacements = new LinkedHashMap<>();
        replacements.put("a", "1");
        replacements.put("b", "2");
        config5.setReplacements(replacements);
        config5.setSpecific(
                Collections.singletonList(
                        new FieldRenamerConfig.SpecificModify("test", "aaa", "other")));
        FieldRenamerTransform transform5 =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config5);
        Assertions.assertEquals("other", transform5.convertName("test", "aaa"));
        Assertions.assertEquals("a", transform5.convertName("test", "a"));
        Assertions.assertEquals("p_1_TEST_s", transform5.convertName("test2", "a_test"));
        Assertions.assertEquals("p_2_TEST_s", transform5.convertName("test2", "b_test"));
        Assertions.assertEquals("p_A2_TEST_s", transform5.convertName("test2", "ab_test"));
    }

    @Test
    void testProduceNewCatalogTable() {
        FieldRenamerConfig config = new FieldRenamerConfig();
        List<FieldRenamerConfig.SpecificModify> specificModifies = new ArrayList<>();
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f1", "f1_new"));
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f2", "f2_new"));
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f3", "f3_new"));
        config.setSpecific(specificModifies);
        FieldRenamerTransform transform =
                new FieldRenamerTransform(Collections.singletonList(DEFAULT_TABLE), config);
        List<CatalogTable> newCatalogTables = transform.getProducedCatalogTables();
        Assertions.assertEquals(1, newCatalogTables.size());
        Assertions.assertEquals("table-x", newCatalogTables.get(0).getTablePath().getTableName());
        Assertions.assertEquals(
                "f1_new", newCatalogTables.get(0).getTableSchema().getColumns().get(0).getName());
        Assertions.assertEquals(
                "f2_new", newCatalogTables.get(0).getTableSchema().getColumns().get(1).getName());
        Assertions.assertEquals(
                "f3_new", newCatalogTables.get(0).getTableSchema().getColumns().get(2).getName());
        Assertions.assertEquals(
                "f1_new",
                newCatalogTables.get(0).getTableSchema().getPrimaryKey().getColumnNames().get(0));
        Assertions.assertEquals(
                "f2_new",
                newCatalogTables
                        .get(0)
                        .getTableSchema()
                        .getConstraintKeys()
                        .get(0)
                        .getColumnNames()
                        .get(0)
                        .getColumnName());
        Assertions.assertEquals(
                "f3_new",
                newCatalogTables
                        .get(0)
                        .getTableSchema()
                        .getConstraintKeys()
                        .get(0)
                        .getColumnNames()
                        .get(1)
                        .getColumnName());

        FieldRenamerConfig config2 = new FieldRenamerConfig();
        List<FieldRenamerConfig.SpecificModify> specificModifies2 = new ArrayList<>();
        specificModifies2.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f1", "f1_new"));
        specificModifies2.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f2", "f1_new"));
        specificModifies2.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f3", "f2"));
        config2.setSpecific(specificModifies2);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new FieldRenamerTransform(
                                                Collections.singletonList(DEFAULT_TABLE), config2)
                                        .getProducedCatalogTables());
        Assertions.assertEquals(
                "ErrorCode:[FIELD_RENAMER-01], ErrorDescription:[The FieldRenamer renamed target field name had duplicate name: '{\"database-x.table-x\":{\"f1\":\"f1_new\",\"f2\":\"f1_new\"}}']",
                exception.getMessage());

        FieldRenamerConfig config3 = new FieldRenamerConfig();
        List<FieldRenamerConfig.SpecificModify> specificModifies3 = new ArrayList<>();
        specificModifies3.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f1", "f2"));
        config3.setSpecific(specificModifies3);
        TransformException exception2 =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new FieldRenamerTransform(
                                                Collections.singletonList(DEFAULT_TABLE), config3)
                                        .getProducedCatalogTables());
        Assertions.assertEquals(
                "ErrorCode:[FIELD_RENAMER-01], ErrorDescription:[The FieldRenamer renamed target field name had duplicate name: '{\"database-x.table-x\":{\"f1\":\"f2\",\"f2\":\"f2\"}}']",
                exception2.getMessage());
    }

    @Test
    void testThrowExpectedException() {
        FieldRenamerConfig config = new FieldRenamerConfig();
        List<FieldRenamerConfig.SpecificModify> specificModifies = new ArrayList<>();
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-y", "f1", "f1_new"));
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-z", "f1", "f1_new"));
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f4", "f2_new"));
        specificModifies.add(
                new FieldRenamerConfig.SpecificModify("database-x.table-x", "f5", "f3_new"));
        config.setSpecific(specificModifies);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new FieldRenamerTransform(
                                                Collections.singletonList(DEFAULT_TABLE), config)
                                        .getProducedCatalogTables());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-07], ErrorDescription:[The 'FieldRenamer' upstream schema not exist table '[\"database-x.table-y\",\"database-x.table-z\"]'，upstream schema not exist fields: '{\"database-x.table-x\":[\"f4\",\"f5\"]}']",
                exception.getMessage());
    }

    @Test
    void testTableEventChange() {
        FieldRenamerConfig config = new FieldRenamerConfig();
        config.setTableMatchRegex("test.*");
        config.setConvertCase(ConvertCase.UPPER);
        config.setReplacements(new LinkedHashMap<>(Collections.singletonMap("abc", "abc_")));
        config.setPrefix("abc");
        config.setSuffix("ee");
        config.setSpecific(
                Collections.singletonList(
                        new FieldRenamerConfig.SpecificModify("database-x.table-x", "f1", "f1_x")));
        FieldRenamerTransform transform =
                new FieldRenamerTransform(
                        Arrays.asList(
                                DEFAULT_TABLE,
                                CatalogTable.of(
                                        TableIdentifier.of("catalog", TablePath.of("test.test2")),
                                        DEFAULT_TABLE)),
                        config);
        AlterTableAddColumnEvent alterTableAddColumnEvent =
                new AlterTableAddColumnEvent(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of(
                                "f1_x",
                                BasicType.LONG_TYPE,
                                null,
                                false,
                                null,
                                null,
                                "int unsigned",
                                false,
                                false,
                                null,
                                null,
                                null),
                        true,
                        null);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> transform.mapSchemaChangeEvent(alterTableAddColumnEvent));
        Assertions.assertEquals(
                "ErrorCode:[FIELD_RENAMER-01], ErrorDescription:[The FieldRenamer renamed target field name had duplicate name: '{\"database-x.table-x\":{\"f1_x\":\"f1_x\",\"f1\":\"f1_x\"}}']",
                exception.getMessage());

        FieldRenamerTransform transform2 =
                new FieldRenamerTransform(
                        Arrays.asList(
                                DEFAULT_TABLE,
                                CatalogTable.of(
                                        TableIdentifier.of("catalog", TablePath.of("test.test2")),
                                        DEFAULT_TABLE)),
                        config);
        AlterTableAddColumnEvent alterTableAddColumnEvent2 =
                new AlterTableAddColumnEvent(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of(
                                "f1_xx",
                                BasicType.LONG_TYPE,
                                null,
                                false,
                                null,
                                null,
                                "int unsigned",
                                false,
                                false,
                                null,
                                null,
                                null),
                        false,
                        "f1");
        AlterTableAddColumnEvent newAlterTableAddColumnEvent2 =
                (AlterTableAddColumnEvent)
                        transform2.mapSchemaChangeEvent(alterTableAddColumnEvent2);
        Assertions.assertEquals("f1_xx", newAlterTableAddColumnEvent2.getColumn().getName());
        Assertions.assertEquals("f1_x", newAlterTableAddColumnEvent2.getAfterColumn());
        AlterTableDropColumnEvent alterTableDropColumnEvent =
                new AlterTableDropColumnEvent(
                        TableIdentifier.of(null, TablePath.of("test.test2")), "f2");
        Assertions.assertEquals(
                "abcF2ee",
                ((AlterTableDropColumnEvent)
                                transform2.mapSchemaChangeEvent(alterTableDropColumnEvent))
                        .getColumn());
        AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                new AlterTableChangeColumnEvent(
                        TableIdentifier.of(null, TablePath.of("test.test2")),
                        "f3",
                        PhysicalColumn.of(
                                "f3_xx",
                                BasicType.LONG_TYPE,
                                null,
                                false,
                                null,
                                null,
                                "int unsigned",
                                false,
                                false,
                                null,
                                null,
                                null),
                        false,
                        "f1");
        AlterTableChangeColumnEvent newAlterTableChangeColumnEvent =
                (AlterTableChangeColumnEvent)
                        transform2.mapSchemaChangeEvent(alterTableChangeColumnEvent);
        Assertions.assertEquals("abcF3ee", newAlterTableChangeColumnEvent.getOldColumn());
        Assertions.assertEquals("abcF1ee", newAlterTableChangeColumnEvent.getAfterColumn());
        Assertions.assertEquals("abcF3_XXee", newAlterTableChangeColumnEvent.getColumn().getName());

        AlterTableChangeColumnEvent alterTableChangeColumnEvent2 =
                new AlterTableChangeColumnEvent(
                        DEFAULT_TABLE.getTableId(),
                        "f1",
                        PhysicalColumn.of(
                                "f1_xt",
                                BasicType.LONG_TYPE,
                                null,
                                false,
                                null,
                                null,
                                "int unsigned",
                                false,
                                false,
                                null,
                                null,
                                null),
                        false,
                        "f2");
        Assertions.assertNull(transform2.mapSchemaChangeEvent(alterTableChangeColumnEvent2));
        List<CatalogTable> catalogTables = transform2.getProducedCatalogTables();
        Assertions.assertEquals(
                "f1_x", catalogTables.get(0).getTableSchema().getColumns().get(2).getName());

        AlterTableNameEvent alterTableNameEvent =
                new AlterTableNameEvent(
                        DEFAULT_TABLE.getTableId(),
                        TableIdentifier.of(
                                DEFAULT_TABLE.getCatalogName(), TablePath.of("test", "test3")));
        transform2.mapSchemaChangeEvent(alterTableNameEvent);
        Assertions.assertEquals(
                "abcF1_XXee",
                transform2
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(0)
                        .getName());
        Assertions.assertEquals(
                "abcF2ee",
                transform2
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(1)
                        .getName());
        Assertions.assertEquals(
                "f1_x",
                transform2
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(2)
                        .getName());
        Assertions.assertEquals(
                "abcF3ee",
                transform2
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(3)
                        .getName());

        AlterTableColumnsEvent alterTableColumnsEvent =
                new AlterTableColumnsEvent(
                        DEFAULT_TABLE.getTableId(),
                        Collections.singletonList(
                                new AlterTableAddColumnEvent(
                                        TableIdentifier.of(
                                                DEFAULT_TABLE.getCatalogName(),
                                                TablePath.of("test", "test2")),
                                        PhysicalColumn.of(
                                                "f1_new",
                                                BasicType.LONG_TYPE,
                                                null,
                                                false,
                                                null,
                                                null,
                                                "int unsigned",
                                                false,
                                                false,
                                                null,
                                                null,
                                                null),
                                        false,
                                        "f1")));
        AlterTableColumnsEvent newAlterTableColumnsEvent =
                (AlterTableColumnsEvent) transform2.mapSchemaChangeEvent(alterTableColumnsEvent);
        Assertions.assertEquals(
                "abcF1_NEWee",
                ((AlterTableAddColumnEvent) newAlterTableColumnsEvent.getEvents().get(0))
                        .getColumn()
                        .getName());
        Assertions.assertEquals(
                "abcF1ee",
                ((AlterTableAddColumnEvent) newAlterTableColumnsEvent.getEvents().get(0))
                        .getAfterColumn());
    }
}
