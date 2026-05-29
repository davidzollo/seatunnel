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

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MongodbRecordUtilsTest {

    @Test
    public void testExtractBsonDocumentWithScientificDate() {
        Schema schema =
                SchemaBuilder.struct().field("fullDocument", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct value =
                new Struct(schema)
                        .put("fullDocument", "{\"createdAt\":{\"$date\":1.680767276146E12}}");

        BsonDocument document =
                MongodbRecordUtils.extractBsonDocument(value, schema, "fullDocument");

        Assertions.assertEquals(1680767276146L, document.getDateTime("createdAt").getValue());
    }
}
