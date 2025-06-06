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

package org.apache.seatunnel.connectors.seatunnel.sapbw.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.sapbw.catalog.SAPBWCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.QueryTableConfig;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBWSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.sapbw.state.SAPBWState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SAPBWSource
        implements SeaTunnelSource<SeaTunnelRow, SAPBWSplit, SAPBWState>,
                SupportParallelism,
                SupportColumnProjection {

    private final SAPBWSourceConfig sourceConfig;
    private final Map<TablePath, CatalogTable> catalogTables;

    public SAPBWSource(ReadonlyConfig readonlyConfig) {
        this.sourceConfig = new SAPBWSourceConfig(readonlyConfig);
        catalogTables = new HashMap<>();
        try (Catalog catalog = new SAPBWCatalogFactory().createCatalog("SAPBW", readonlyConfig)) {
            catalog.open();
            for (Map.Entry<TablePath, QueryTableConfig> entry :
                    sourceConfig.getQueryTableConfigs().entrySet()) {
                QueryTableConfig tableConfig = entry.getValue();
                TablePath tablePath = entry.getKey();
                catalogTables.put(
                        tablePath,
                        catalog.getTable(tablePath, tableConfig.getDimensionsAndMeasures()));
            }
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(catalogTables.values());
    }

    @Override
    public SourceSplitEnumerator<SAPBWSplit, SAPBWState> createEnumerator(
            SourceSplitEnumerator.Context<SAPBWSplit> enumeratorContext) {
        return new SAPBWSplitEnumerator(enumeratorContext, sourceConfig, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<SAPBWSplit, SAPBWState> restoreEnumerator(
            SourceSplitEnumerator.Context<SAPBWSplit> enumeratorContext,
            SAPBWState checkpointState) {
        return new SAPBWSplitEnumerator(
                enumeratorContext, sourceConfig, checkpointState.getAssignedSplits());
    }

    @Override
    public SourceReader<SeaTunnelRow, SAPBWSplit> createReader(SourceReader.Context readerContext) {
        return new SAPBWSourceReader(readerContext, sourceConfig, catalogTables);
    }

    @Override
    public String getPluginName() {
        return SAPBWSourceFactory.IDENTIFIER;
    }
}
