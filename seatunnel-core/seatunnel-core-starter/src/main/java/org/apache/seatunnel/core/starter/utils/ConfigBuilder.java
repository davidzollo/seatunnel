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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.configuration.ConfigAdapter;
import org.apache.seatunnel.core.starter.enums.CryptoMode;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.core.starter.utils.ConfigShadeUtils.DEFAULT_SENSITIVE_KEYWORDS;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private ConfigBuilder() {
        // utility class and cannot be instantiated
    }

    private static Config ofInner(@NonNull Path filePath, CryptoMode cryptoMode) {
        Config config =
                ConfigFactory.parseFile(filePath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return ConfigShadeUtils.decryptConfig(config, cryptoMode);
    }

    public static Config of(@NonNull String filePath) {
        Path path = Paths.get(filePath);
        return of(path);
    }

    public static Config of(@NonNull String filePath, CryptoMode cryptoMode) {
        Path path = Paths.get(filePath);
        return of(path, cryptoMode);
    }

    public static Config of(@NonNull Path filePath, CryptoMode cryptoMode) {
        log.info("Loading config file from path: {}", filePath);
        Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);
        Config config =
                adapterSupplier
                        .map(adapter -> of(adapter, filePath, cryptoMode))
                        .orElseGet(() -> ofInner(filePath, cryptoMode));
        return config;
    }

    public static Config of(@NonNull Path filePath) {
        return of(filePath, CryptoMode.DEFAULT);
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        return of(objectMap, false);
    }

    public static Config of(@NonNull Map<String, Object> objectMap, boolean isEncrypt) {
        return of(objectMap, isEncrypt, CryptoMode.DEFAULT);
    }

    public static Config of(
            @NonNull Map<String, Object> objectMap, boolean isEncrypt, CryptoMode cryptoMode) {
        log.info("Loading config file from objectMap");
        Config config =
                ConfigFactory.parseMap(objectMap)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        if (!isEncrypt) {
            config = ConfigShadeUtils.decryptConfig(config, cryptoMode);
        }
        return config;
    }

    public static Config of(@NonNull ConfigAdapter configAdapter, @NonNull Path filePath) {
        return of(configAdapter, filePath, CryptoMode.DEFAULT);
    }

    public static Config of(
            @NonNull ConfigAdapter configAdapter, @NonNull Path filePath, CryptoMode cryptoMode) {
        log.info("With config adapter spi {}", configAdapter.getClass().getName());
        try {
            Map<String, Object> flattenedMap = configAdapter.loadConfig(filePath);
            Config config = ConfigFactory.parseMap(flattenedMap);
            return ConfigShadeUtils.decryptConfig(config, cryptoMode);
        } catch (Exception warn) {
            log.warn(
                    "Loading config failed with spi {}, fallback to HOCON loader.",
                    configAdapter.getClass().getName());
            return ofInner(filePath, cryptoMode);
        }
    }

    public static Map<String, Object> configDesensitization(Map<String, Object> configMap) {
        return configMap.entrySet().stream()
                .collect(
                        LinkedHashMap::new,
                        (m, p) -> {
                            String key = p.getKey();
                            Object value = p.getValue();
                            if (DEFAULT_SENSITIVE_KEYWORDS.contains(key.toLowerCase())) {
                                m.put(key, "******");
                            } else {
                                if (value instanceof Map<?, ?>) {
                                    m.put(key, configDesensitization((Map<String, Object>) value));
                                } else if (value instanceof List<?>) {
                                    List<?> listValue = (List<?>) value;
                                    List<Object> newList =
                                            listValue.stream()
                                                    .map(
                                                            v -> {
                                                                if (v instanceof Map<?, ?>) {
                                                                    return configDesensitization(
                                                                            (Map<String, Object>)
                                                                                    v);
                                                                } else {
                                                                    return v;
                                                                }
                                                            })
                                                    .collect(Collectors.toList());
                                    m.put(key, newList);
                                } else {
                                    m.put(key, value);
                                }
                            }
                        },
                        LinkedHashMap::putAll);
    }
}
