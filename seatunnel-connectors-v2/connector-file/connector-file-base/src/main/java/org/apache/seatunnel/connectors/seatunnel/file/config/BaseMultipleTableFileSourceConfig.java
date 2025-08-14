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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseMultipleTableFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<BaseFileSourceConfig> fileSourceConfigs;

    public BaseMultipleTableFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        if (fileSourceRootConfig.getOptional(BaseSourceConfigOptions.TABLE_CONFIGS).isPresent()) {
            parseFromFileSourceConfigs(fileSourceRootConfig);
        } else {
            parseFromFileSourceConfig(fileSourceRootConfig);
        }
    }

    private void parseFromFileSourceConfigs(ReadonlyConfig fileSourceRootConfig) {
        List<Map<String, Object>> rawTableConfigs =
                fileSourceRootConfig.get(BaseSourceConfigOptions.TABLE_CONFIGS);

        mergeOptionsToTableMaps(
                rawTableConfigs,
                fileSourceRootConfig,
                Lists.newArrayList(BaseSourceConfigOptions.FILE_FILTER_PATTERN));

        List<ReadonlyConfig> tableConfig =
                rawTableConfigs.stream().map(ReadonlyConfig::fromMap).collect(Collectors.toList());

        this.fileSourceConfigs =
                tableConfig.stream().map(this::getBaseSourceConfig).collect(Collectors.toList());
    }

    public static <T> void mergeOptionsToTableMaps(
            List<Map<String, Object>> tableMaps,
            ReadonlyConfig rootConfig,
            List<Option<T>> optionKeys) {
        for (Option<T> optionKey : optionKeys) {
            if (!rootConfig.getOptional(optionKey).isPresent()) {
                continue;
            }
            Object value = rootConfig.get(optionKey);
            for (Map<String, Object> table : tableMaps) {
                if (!table.containsKey(optionKey.key())) {
                    table.put(optionKey.key(), value);
                }
            }
        }
    }

    public abstract BaseFileSourceConfig getBaseSourceConfig(ReadonlyConfig readonlyConfig);

    private void parseFromFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        this.fileSourceConfigs = Lists.newArrayList(getBaseSourceConfig(fileSourceRootConfig));
    }
}
