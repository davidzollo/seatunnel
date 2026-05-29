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

package mongodb.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender.MongoDBConnectorDeserializationSchema;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

public class MongoDBConnectorDeserializationSchemaTest {

    @Test
    public void testConvertInt32ToBigint() {
        CatalogTable catalogTable = tableWithField("status", BasicType.LONG_TYPE);
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(
                        Collections.singletonList(catalogTable), Collections.emptyMap());

        SeaTunnelRow row =
                (SeaTunnelRow)
                        schema.createConverter(Collections.singletonList(catalogTable))
                                .get(catalogTable.getTablePath().toString())
                                .convert(new BsonDocument("status", new BsonInt32(1)));

        Assertions.assertEquals(1L, row.getField(0));
    }

    @Test
    public void testConvertBsonValuesToStringWithoutValueWrapper() {
        CatalogTable catalogTable =
                stringTable(
                        "int32_field",
                        "int64_field",
                        "double_field",
                        "decimal_field",
                        "bool_field",
                        "date_field",
                        "timestamp_field",
                        "array_field",
                        "object_field",
                        "binary_field",
                        "regex_field",
                        "javascript_field",
                        "min_key_field",
                        "max_key_field");
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(
                        Collections.singletonList(catalogTable), Collections.emptyMap());

        SeaTunnelRow row =
                (SeaTunnelRow)
                        schema.createConverter(Collections.singletonList(catalogTable))
                                .get(catalogTable.getTablePath().toString())
                                .convert(
                                        new BsonDocument()
                                                .append("int32_field", new BsonInt32(1))
                                                .append("int64_field", new BsonInt64(2L))
                                                .append("double_field", new BsonDouble(1.23D))
                                                .append(
                                                        "decimal_field",
                                                        new BsonDecimal128(
                                                                new Decimal128(
                                                                        new BigDecimal(
                                                                                "1234567890.123456789"))))
                                                .append("bool_field", BsonBoolean.TRUE)
                                                .append(
                                                        "date_field",
                                                        new BsonDateTime(1778830200000L))
                                                .append(
                                                        "timestamp_field",
                                                        new BsonTimestamp(1765796029, 1))
                                                .append(
                                                        "array_field",
                                                        new BsonArray(
                                                                Arrays.asList(
                                                                        new BsonInt32(1),
                                                                        new BsonString("abc"),
                                                                        new BsonInt64(2L))))
                                                .append(
                                                        "object_field",
                                                        new BsonDocument(
                                                                        "name",
                                                                        new BsonString(
                                                                                "nested object"))
                                                                .append("age", new BsonInt32(18)))
                                                .append(
                                                        "binary_field",
                                                        new BsonBinary("Hello".getBytes()))
                                                .append(
                                                        "regex_field",
                                                        new BsonRegularExpression("abc", "i"))
                                                .append(
                                                        "javascript_field",
                                                        new BsonJavaScript(
                                                                "function() { return 1; }"))
                                                .append("min_key_field", new BsonMinKey())
                                                .append("max_key_field", new BsonMaxKey()));

        Assertions.assertEquals("1", row.getField(0));
        Assertions.assertEquals("2", row.getField(1));
        Assertions.assertEquals("1.23", row.getField(2));
        Assertions.assertEquals("1234567890.123456789", row.getField(3));
        Assertions.assertEquals("true", row.getField(4));
        Assertions.assertEquals("2026-05-15T07:30:00Z", row.getField(5));
        Assertions.assertEquals("{\"$timestamp\": {\"t\": 1765796029, \"i\": 1}}", row.getField(6));
        Assertions.assertEquals("[1, \"abc\", 2]", row.getField(7));
        Assertions.assertEquals("{\"name\": \"nested object\", \"age\": 18}", row.getField(8));
        Assertions.assertEquals(
                "{\"$binary\": {\"base64\": \"SGVsbG8=\", \"subType\": \"00\"}}", row.getField(9));
        Assertions.assertEquals(
                "{\"$regularExpression\": {\"pattern\": \"abc\", \"options\": \"i\"}}",
                row.getField(10));
        Assertions.assertEquals("{\"$code\": \"function() { return 1; }\"}", row.getField(11));
        Assertions.assertEquals("{\"$minKey\": 1}", row.getField(12));
        Assertions.assertEquals("{\"$maxKey\": 1}", row.getField(13));

        for (int i = 0; i < row.getArity(); i++) {
            Assertions.assertFalse(String.valueOf(row.getField(i)).contains("\"_value\""));
        }
    }

    @Test
    public void testKeepExistingStringObjectIdDocumentAndNullStringConversion() {
        CatalogTable catalogTable =
                stringTable("string_field", "_id", "object_field", "null_field");
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(
                        Collections.singletonList(catalogTable), Collections.emptyMap());
        ObjectId objectId = new ObjectId("665000000000000000000001");

        SeaTunnelRow row =
                (SeaTunnelRow)
                        schema.createConverter(Collections.singletonList(catalogTable))
                                .get(catalogTable.getTablePath().toString())
                                .convert(
                                        new BsonDocument()
                                                .append(
                                                        "string_field",
                                                        new BsonString("hello mongo"))
                                                .append("_id", new BsonObjectId(objectId))
                                                .append(
                                                        "object_field",
                                                        new BsonDocument(
                                                                        "name",
                                                                        new BsonString(
                                                                                "nested object"))
                                                                .append("age", new BsonInt32(18)))
                                                .append("null_field", BsonNull.VALUE));

        Assertions.assertEquals("hello mongo", row.getField(0));
        Assertions.assertEquals("665000000000000000000001", row.getField(1));
        Assertions.assertEquals("{\"name\": \"nested object\", \"age\": 18}", row.getField(2));
        Assertions.assertNull(row.getField(3));
    }

    private static CatalogTable stringTable(String... fieldNames) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String fieldName : fieldNames) {
            builder.column(
                    PhysicalColumn.of(
                            fieldName, BasicType.STRING_TYPE, (Long) null, true, null, null));
        }
        return table(builder);
    }

    private static CatalogTable tableWithField(
            String fieldName, org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> fieldType) {
        return table(
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        fieldName, fieldType, (Long) null, true, null, null)));
    }

    private static CatalogTable table(TableSchema.Builder builder) {
        return CatalogTable.of(
                TableIdentifier.of("MongoDB", "default", "alltype_string"),
                builder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }
}
