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

package org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.wal;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.kingbase.source.reader.KingbaseSourceFetchTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.kingbase.KingbaseOffsetContext;
import io.debezium.connector.kingbase.KingbaseStreamingChangeEventSource;
import io.debezium.connector.kingbase.connection.Lsn;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KingbaseWalFetchTask implements FetchTask<SourceSplitBase> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseWalFetchTask.class);

    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;
    private Long lastCommitLsn;
    private KingbaseStreamingChangeEventSource streamingChangeEventSource;
    private KingbaseOffsetContext offsetContext;

    public KingbaseWalFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        KingbaseSourceFetchTaskContext sourceFetchContext =
                (KingbaseSourceFetchTaskContext) context;
        taskRunning = true;

        streamingChangeEventSource =
                new KingbaseStreamingChangeEventSource(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getSnapshotter(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getKingbaseEventDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        Clock.SYSTEM,
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getReplicationConnection());

        offsetContext = sourceFetchContext.getOffsetContext();

        TransactionLogSplitChangeEventSourceContext changeEventSourceContext =
                new TransactionLogSplitChangeEventSourceContext();

        LOGGER.info(
                "Start streaming change event source for kingbase wal split: {}",
                split.getStartupOffset().toString());

        streamingChangeEventSource.execute(
                changeEventSourceContext, sourceFetchContext.getPartition(), offsetContext);
    }

    public void commitCurrentOffset(LsnOffset offset) {
        if (streamingChangeEventSource != null && offset != null) {

            // only extracting and storing the lsn of the last commit checkpoint
            Long commitLsn = offset.getLsn().asLong();
            if (commitLsn != null
                    && (lastCommitLsn == null
                            || Lsn.valueOf(commitLsn).compareTo(Lsn.valueOf(lastCommitLsn)) > 0)) {
                lastCommitLsn = commitLsn;

                Map<String, Object> offsets = new HashMap<>();
                offsets.put(KingbaseOffsetContext.LAST_COMMIT_LSN_KEY, lastCommitLsn);
                LOGGER.info("Committing offset {} for {}", Lsn.valueOf(lastCommitLsn), split);
                streamingChangeEventSource.commitOffset(offsets);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    private class TransactionLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
