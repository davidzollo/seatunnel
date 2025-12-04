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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.enums.CryptoMode;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;

/** Config shade utilities */
@Slf4j
public final class ConfigShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";

    private static final String SENSITIVE_FIELDS_CONFIG = "sensitive-fields.conf";

    /**
     * Default sensitive keywords loaded from configuration file These keywords will be used for
     * exact matching of sensitive fields
     */
    public static final List<String> DEFAULT_SENSITIVE_KEYWORDS;

    static {
        DEFAULT_SENSITIVE_KEYWORDS = loadSensitiveFieldsFromConfig();
    }

    /**
     * Load sensitive fields from configuration file Priority: 1. External config directory
     * (SEATUNNEL_HOME/config) 2. Classpath resource
     *
     * @return list of sensitive field names
     */
    private static List<String> loadSensitiveFieldsFromConfig() {
        InputStream inputStream = null;
        String configSource = null;

        try {
            // Try to load from external config directory first
            String seatunnelHome = System.getenv("SEATUNNEL_HOME");
            if (seatunnelHome != null && !seatunnelHome.isEmpty()) {
                Path externalConfigPath =
                        Paths.get(seatunnelHome, "config", SENSITIVE_FIELDS_CONFIG);
                if (Files.exists(externalConfigPath)) {
                    inputStream = Files.newInputStream(externalConfigPath);
                    configSource = "external config: " + externalConfigPath;
                    log.info(
                            "Loading sensitive fields from external config: {}",
                            externalConfigPath);
                }
            }

            // Fallback to classpath resource
            if (inputStream == null) {
                inputStream =
                        ConfigShadeUtils.class
                                .getClassLoader()
                                .getResourceAsStream(SENSITIVE_FIELDS_CONFIG);
                if (inputStream != null) {
                    configSource = "classpath resource";
                    log.info("Loading sensitive fields from classpath resource");
                }
            }

            if (inputStream == null) {
                log.warn(
                        "Sensitive fields configuration file not found in external config or classpath: {}, using empty list",
                        SENSITIVE_FIELDS_CONFIG);
                return Collections.emptyList();
            }

            Config config =
                    ConfigFactory.parseReader(
                            new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8));

            if (!config.hasPath("sensitive-fields")) {
                log.warn(
                        "No 'sensitive-fields' key found in configuration file from {}",
                        configSource);
                return Collections.emptyList();
            }

            List<String> fields = config.getStringList("sensitive-fields");
            log.info("Loaded {} sensitive fields from {}", fields.size(), configSource);
            return fields;
        } catch (Exception e) {
            log.error("Failed to load sensitive fields from configuration file", e);
            return Collections.emptyList();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    log.warn("Failed to close input stream", e);
                }
            }
        }
    }

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
                });
        log.info("Load config shade spi: {}", CONFIG_SHADES.keySet());
    }

    private static class DefaultConfigShade implements ConfigShade {
        private static final String IDENTIFIER = "default";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return content;
        }

        @Override
        public String decrypt(String content) {
            return content;
        }
    }

    public static String encryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        try {
            return configShade.encrypt(content);
        } catch (Exception e) {
            log.warn(
                    "Failed to encrypt content with identifier '{}', treating as plain text: {}",
                    identifier,
                    e.getMessage());
            return content;
        }
    }

    public static String decryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        try {
            return configShade.decrypt(content);
        } catch (Exception e) {
            log.warn(
                    "Failed to decrypt content with identifier '{}', treating as plain text: {}",
                    identifier,
                    e.getMessage());
            return content;
        }
    }

    public static Config decryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        return decryptConfig(identifier, config, CryptoMode.DEFAULT);
    }

    public static Config decryptConfig(Config config, CryptoMode cryptoMode) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        return decryptConfig(identifier, config, cryptoMode);
    }

    public static Config encryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        return encryptConfig(identifier, config, CryptoMode.DEFAULT);
    }

    public static Config encryptConfig(Config config, CryptoMode cryptoMode) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        return encryptConfig(identifier, config, cryptoMode);
    }

    public static Config decryptConfig(String identifier, Config config, CryptoMode cryptoMode) {
        return processConfig(identifier, config, true, cryptoMode);
    }

    public static Config encryptConfig(String identifier, Config config, CryptoMode cryptoMode) {
        return processConfig(identifier, config, false, cryptoMode);
    }

    @SuppressWarnings("unchecked")
    private static Config processConfig(
            String identifier, Config config, boolean isDecrypted, CryptoMode cryptoMode) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        List<String> sensitiveOptions = new ArrayList<>(DEFAULT_SENSITIVE_KEYWORDS);
        sensitiveOptions.addAll(Arrays.asList(configShade.sensitiveOptions()));
        if (cryptoMode == CryptoMode.DEFAULT) {
            Set<String> uniqueKeys = new HashSet<>(sensitiveOptions);
            sensitiveOptions.clear();
            sensitiveOptions.addAll(uniqueKeys);
        }
        BiFunction<String, Object, String> processFunction =
                (key, value) -> {
                    try {
                        if (isDecrypted) {
                            return configShade.decrypt(value.toString());
                        } else {
                            return configShade.encrypt(value.toString());
                        }
                    } catch (Exception e) {
                        log.warn(
                                "Failed to {} content for key '{}', treating as plain text: {}",
                                isDecrypted ? "decrypt" : "encrypt",
                                key,
                                e.getMessage());
                        return value.toString();
                    }
                };
        String jsonString = config.root().render(ConfigRenderOptions.concise());
        ObjectNode jsonNodes = JsonUtils.parseObject(jsonString);
        Map<String, Object> configMap = JsonUtils.toMap(jsonNodes);
        List<Map<String, Object>> sources =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SOURCE);
        List<Map<String, Object>> sinks =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SINK);
        Preconditions.checkArgument(
                !sources.isEmpty(), "Miss <Source> config! Please check the config file.");
        Preconditions.checkArgument(
                !sinks.isEmpty(), "Miss <Sink> config! Please check the config file.");
        sources.forEach(
                source -> {
                    processMapRecursively(source, sensitiveOptions, processFunction);
                });
        sinks.forEach(
                sink -> {
                    processMapRecursively(sink, sensitiveOptions, processFunction);
                });
        configMap.put(Constants.SOURCE, sources);
        configMap.put(Constants.SINK, sinks);
        return ConfigFactory.parseMap(configMap);
    }

    /**
     * Recursively process a map to encrypt/decrypt sensitive fields using exact matching
     *
     * @param map the map to process
     * @param sensitiveOptions list of sensitive field names for exact matching
     * @param processFunction the encryption/decryption function
     */
    @SuppressWarnings("unchecked")
    private static void processMapRecursively(
            Map<String, Object> map,
            List<String> sensitiveOptions,
            BiFunction<String, Object, String> processFunction) {
        if (map == null) {
            return;
        }

        // Collect keys to process to avoid ConcurrentModificationException
        List<String> keysToProcess = new ArrayList<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Handle nested maps
            if (value instanceof Map) {
                processMapRecursively(
                        (Map<String, Object>) value, sensitiveOptions, processFunction);
            }
            // Handle lists of maps
            else if (value instanceof List) {
                List<?> list = (List<?>) value;
                for (Object item : list) {
                    if (item instanceof Map) {
                        processMapRecursively(
                                (Map<String, Object>) item, sensitiveOptions, processFunction);
                    }
                }
            }
            // Handle string values - exact match only
            else if (value instanceof String) {
                if (sensitiveOptions.contains(key)) {
                    keysToProcess.add(key);
                }
            }
        }

        // Process collected keys
        for (String key : keysToProcess) {
            map.computeIfPresent(key, processFunction);
        }
    }

    public static class Base64ConfigShade implements ConfigShade {

        private static final Base64.Encoder ENCODER = Base64.getEncoder();

        private static final Base64.Decoder DECODER = Base64.getDecoder();

        private static final String IDENTIFIER = "base64";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String decrypt(String content) {
            return new String(DECODER.decode(content));
        }
    }

    public static class AES256ConfigShade implements ConfigShade {

        private static final String ALGORITHM = "AES";
        private static final String MODE = "AES/CBC/PKCS5Padding";
        private static final String SECRET_KEY = "uBdUx26vPkDKb997d5NkjFoNcKWLwang";
        private static final byte[] KEY_VI = "c798GqWXPK2QUlMc".getBytes();
        private static final String IDENTIFIER = "aes256";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @SneakyThrows
        @Override
        public String encrypt(String content) {
            SecretKey secretKey = new SecretKeySpec(SECRET_KEY.getBytes(), ALGORITHM);
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(MODE);
            cipher.init(
                    javax.crypto.Cipher.ENCRYPT_MODE,
                    secretKey,
                    new javax.crypto.spec.IvParameterSpec(KEY_VI));
            byte[] byteEncode = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] byteAES = cipher.doFinal(byteEncode);
            return Base64.getEncoder().encodeToString(byteAES);
        }

        @SneakyThrows
        @Override
        public String decrypt(String content) {
            javax.crypto.SecretKey secretKey =
                    new javax.crypto.spec.SecretKeySpec(SECRET_KEY.getBytes(), ALGORITHM);
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(MODE);
            cipher.init(
                    javax.crypto.Cipher.DECRYPT_MODE,
                    secretKey,
                    new javax.crypto.spec.IvParameterSpec(KEY_VI));
            byte[] byteContent = Base64.getDecoder().decode(content);
            byte[] byteDecode = cipher.doFinal(byteContent);
            return new String(byteDecode, java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
