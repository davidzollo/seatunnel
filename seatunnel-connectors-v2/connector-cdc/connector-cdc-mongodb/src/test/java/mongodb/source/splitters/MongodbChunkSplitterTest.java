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

package mongodb.source.splitters;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.splitters.MongodbChunkSplitter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collection;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;

/** Covers the single-split fallback when MongoDB concurrent read is disabled. */
public class MongodbChunkSplitterTest {

    /** Verify the splitter returns a single snapshot split without collection probing. */
    @Test
    public void testGenerateSingleSplitWhenConcurrentReadDisabled() {
        MongodbSourceConfig sourceConfig =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts("127.0.0.1:1")
                        .connectionOptions("serverSelectionTimeoutMS=50&connectTimeoutMS=50")
                        .enableConcurrentRead(false)
                        .whereConditionClause("{\"tenant\":1}")
                        .validate()
                        .create(0);

        Collection<SnapshotSplit> splits =
                new MongodbChunkSplitter(sourceConfig)
                        .generateSplits(TableId.parse("test_db.test_collection"));

        Assertions.assertEquals(1, splits.size());
        SnapshotSplit split = splits.iterator().next();
        Assertions.assertEquals("test_db.test_collection:0", split.splitId());
        Assertions.assertEquals("{\"tenant\":1}", split.getWhereConditionClause());
        Assertions.assertArrayEquals(minLowerBoundOfId(), split.getSplitStart());
        Assertions.assertArrayEquals(maxUpperBoundOfId(), split.getSplitEnd());
    }
}
