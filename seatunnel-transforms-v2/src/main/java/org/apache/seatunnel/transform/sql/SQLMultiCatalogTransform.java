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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogSupportTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformExceptionUtil;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SQLMultiCatalogTransform extends AbstractMultiCatalogSupportTransform {

    private final ReadonlyConfig config;

    public SQLMultiCatalogTransform(List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return SQLTransform.PLUGIN_NAME;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return getProducedCatalogTables().get(0);
    }

    @Override
    protected Optional<SeaTunnelTransform<SeaTunnelRow>> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return SQLTransformConfig.of(config, inputCatalogTable)
                .map(sqlConfig -> new SQLTransform(sqlConfig, config, inputCatalogTable));
    }

    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        throw new UnsupportedOperationException("SQL Transform not support DDL");
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        preCheckForConfig(inputCatalogTables);
        return outputCatalogTables;
    }

    private void preCheckForConfig(List<CatalogTable> inputCatalogTables) {
        final List<SQLTransformConfig.TableTransforms> tableTransforms =
                config.get(SQLTransformConfig.MULTI_TABLES);
        if (CollectionUtils.isEmpty(tableTransforms)) {
            return;
        }
        final List<String> fullTableNameList =
                inputCatalogTables.stream()
                        .map(table -> table.getTablePath().getFullName())
                        .collect(Collectors.toList());
        TransformExceptionUtil.withErrorCheck(
                SQLTransform.PLUGIN_NAME,
                tableTransforms.iterator(),
                entry -> {
                    final String tablePath = entry.getTablePath();
                    if (!fullTableNameList.contains(tablePath)) {
                        throw TransformCommonError.cannotFindInputTableError(
                                SQLTransform.PLUGIN_NAME, tablePath);
                    }
                });
    }
}
