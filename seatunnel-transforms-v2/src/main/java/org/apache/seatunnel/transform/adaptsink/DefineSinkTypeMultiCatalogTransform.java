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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogSupportTransform;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefineSinkTypeMultiCatalogTransform extends AbstractMultiCatalogSupportTransform {

    private final ReadonlyConfig config;

    public DefineSinkTypeMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return DefineSinkTypeTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected Optional<SeaTunnelTransform<SeaTunnelRow>> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return DefineSinkTypeTransformConfig.of(config, inputCatalogTable.getTablePath())
                .map(c -> new DefineSinkTypeTransform(c, inputCatalogTable));
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        preCheckForConfig(inputCatalogTables);
        return outputCatalogTables;
    }

    private void preCheckForConfig(List<CatalogTable> inputCatalogTables) {
        final List<DefineSinkTypeTransformConfig.TableTransforms> tableTransforms =
                config.get(DefineSinkTypeTransformConfig.MULTI_TABLES);
        if (CollectionUtils.isEmpty(tableTransforms)) {
            return;
        }
        final List<String> fullTableNameList =
                inputCatalogTables.stream()
                        .map(table -> table.getTablePath().getFullName())
                        .collect(Collectors.toList());
        tableTransforms.forEach(
                table -> {
                    if (!fullTableNameList.contains(table.getTablePath())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Table %s not found in input tables %s",
                                        table.getTablePath(), fullTableNameList));
                    }
                });
    }
}
