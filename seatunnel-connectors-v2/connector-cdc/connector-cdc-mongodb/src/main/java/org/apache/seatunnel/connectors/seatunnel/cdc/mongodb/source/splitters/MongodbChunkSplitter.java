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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.splitters;

import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;

import io.debezium.relational.TableId;

import java.util.Collection;
import java.util.Collections;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;

public class MongodbChunkSplitter implements ChunkSplitter {

    private final MongodbSourceConfig sourceConfig;

    public MongodbChunkSplitter(MongodbSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId collectionId) {
        if (!sourceConfig.isEnableConcurrentRead()) {
            return Collections.singletonList(createSingleSplit(collectionId));
        }
        SplitContext splitContext = SplitContext.of(sourceConfig, collectionId);
        SplitStrategy splitStrategy =
                splitContext.isShardedCollection()
                        ? ShardedSplitStrategy.INSTANCE
                        : SplitVectorSplitStrategy.INSTANCE;
        return splitStrategy.split(splitContext);
    }

    /** Skip collection metadata probing when concurrent snapshot splitting is disabled. */
    private SnapshotSplit createSingleSplit(TableId collectionId) {
        return new SnapshotSplit(
                SingleSplitStrategy.INSTANCE.splitId(collectionId, 0),
                collectionId,
                SingleSplitStrategy.INSTANCE.shardKeysToRowType(Collections.singleton(ID_FIELD)),
                minLowerBoundOfId(),
                maxUpperBoundOfId(),
                0,
                1,
                false,
                sourceConfig.getWhereConditionClause());
    }
}
