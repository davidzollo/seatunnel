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

package org.apache.seatunnel.transform.mapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogSupportTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformExceptionUtil;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;

public class MapperMultiCatalogTransform extends AbstractMultiCatalogSupportTransform {

    public MapperMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    protected Optional<SeaTunnelTransform<SeaTunnelRow>> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return Optional.of(new MapperTransform(config, inputCatalogTable));
    }

    @Override
    public String getPluginName() {
        return MapperTransform.PLUGIN_NAME;
    }

    @Override
    protected void preCheckConfig(ReadonlyConfig config) {
        List<MapperConfig.SpecificModify> specificModifies = config.get(MapperConfig.SPECIFIC);
        if (CollectionUtils.isEmpty(specificModifies)) {
            return;
        }

        TransformExceptionUtil.withErrorCheck(
                getPluginName(),
                specificModifies.iterator(),
                modify -> {
                    // Reuse the per-table rule matching so the pre-check accepts exactly the
                    // rules that MapperTransform binds at runtime, otherwise a rule could pass
                    // validation here and still be skipped or wrongly bound during execution
                    boolean found =
                            inputCatalogTables.stream()
                                    .anyMatch(
                                            table ->
                                                    MapperTransform.matchesInputTable(
                                                            modify.getInputName(),
                                                            table.getTableId()));

                    if (!found) {
                        throw TransformCommonError.cannotFindInputTableError(
                                getPluginName(), modify.getInputName());
                    }
                });
    }
}
