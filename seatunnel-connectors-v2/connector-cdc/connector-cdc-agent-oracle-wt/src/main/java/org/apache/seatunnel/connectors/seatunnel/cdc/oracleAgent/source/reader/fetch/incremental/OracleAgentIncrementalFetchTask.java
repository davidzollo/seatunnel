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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.incremental;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.OracleAgentSourceFetchTaskContext;

import io.debezium.connector.oracle.OracleAgentStreamingChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OracleAgentIncrementalFetchTask implements FetchTask<SourceSplitBase> {

    private OracleAgentSourceConfig oracleAgentSourceConfig;
    private final IncrementalSplit incrementalSplit;
    private volatile boolean taskRunning = false;

    public OracleAgentIncrementalFetchTask(
            OracleAgentSourceConfig oracleAgentSourceConfig, IncrementalSplit split) {
        this.oracleAgentSourceConfig = oracleAgentSourceConfig;
        this.incrementalSplit = split;
    }

    @Override
    public void execute(Context context) {
        taskRunning = true;
        OracleAgentSourceFetchTaskContext sourceFetchContext =
                (OracleAgentSourceFetchTaskContext) context;

        OracleAgentStreamingChangeEventSource oracleAgentStreamingChangeEventSource =
                new OracleAgentStreamingChangeEventSource(
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        incrementalSplit.getTableIds(),
                        sourceFetchContext.getSourceConfig(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema());

        Oracle9BridgeIncrementalChangeEventSourceContext changeEventSourceContext =
                new Oracle9BridgeIncrementalChangeEventSourceContext();
        oracleAgentStreamingChangeEventSource.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown the OracleAgentIncrementalFetchTask");
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return incrementalSplit;
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for binlog split task.
     */
    private class Oracle9BridgeIncrementalChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
