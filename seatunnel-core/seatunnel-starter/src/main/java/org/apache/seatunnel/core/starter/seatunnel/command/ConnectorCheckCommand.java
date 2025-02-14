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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ConnectorCheckCommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.utils.SeaTunnelConnectorI18n;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The command to check the connector. */
public class ConnectorCheckCommand implements Command<ConnectorCheckCommandArgs> {
    private static final Map<PluginType, PluginDiscovery> DISCOVERY_MAP = new HashMap();
    private ConnectorCheckCommandArgs connectorCheckCommandArgs;

    public ConnectorCheckCommand(ConnectorCheckCommandArgs connectorCheckCommandArgs) {
        this.connectorCheckCommandArgs = connectorCheckCommandArgs;
        this.DISCOVERY_MAP.put(PluginType.SOURCE, new SeaTunnelSourcePluginDiscovery());
        this.DISCOVERY_MAP.put(PluginType.SINK, new SeaTunnelSinkPluginDiscovery());
        this.DISCOVERY_MAP.put(PluginType.TRANSFORM, new SeaTunnelTransformPluginDiscovery());
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        PluginType pluginType = connectorCheckCommandArgs.getPluginType();
        // Print plugins(connectors and transforms)
        if (connectorCheckCommandArgs.isListConnectors()) {
            if (Objects.isNull(pluginType)) {
                DISCOVERY_MAP
                        .entrySet()
                        .forEach(
                                pluginTypePluginDiscoveryEntry ->
                                        printSupportedPlugins(
                                                pluginTypePluginDiscoveryEntry.getKey(),
                                                pluginTypePluginDiscoveryEntry
                                                        .getValue()
                                                        .getPlugins()));
            } else {
                printSupportedPlugins(pluginType, DISCOVERY_MAP.get(pluginType).getPlugins());
            }
        }

        String pluginIdentifier = connectorCheckCommandArgs.getPluginIdentifier();
        // print option rule of the connector
        if (StringUtils.isNoneBlank(pluginIdentifier)) {
            if (Objects.isNull(pluginType)) {
                DISCOVERY_MAP
                        .entrySet()
                        .forEach(
                                pluginTypePluginDiscoveryEntry -> {
                                    printOptionRulesByPluginTypeAndIdentifier(
                                            pluginTypePluginDiscoveryEntry.getValue(),
                                            pluginIdentifier);
                                });
            } else {
                printOptionRulesByPluginTypeAndIdentifier(
                        DISCOVERY_MAP.get(pluginType), pluginIdentifier);
            }
        }
    }

    private void printOptionRulesByPluginTypeAndIdentifier(
            PluginDiscovery DISCOVERY_MAP, String pluginIdentifier) {
        ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> triple =
                DISCOVERY_MAP.getOptionRules(pluginIdentifier);
        if (Objects.nonNull(triple.getLeft())) {
            printOptionRules(triple.getLeft(), triple.getMiddle(), triple.getRight());
        }
    }

    private void printSupportedPlugins(
            PluginType pluginType, LinkedHashMap<PluginIdentifier, OptionRule> plugins) {
        System.out.println(StringUtils.LF + StringUtils.capitalize(pluginType.getType()));
        String supportedPlugins =
                plugins.keySet().stream()
                        .map(pluginIdentifier -> pluginIdentifier.getPluginName())
                        .collect(Collectors.joining(StringUtils.SPACE));
        System.out.println(supportedPlugins + StringUtils.LF);
    }

    private void printOptionRules(
            PluginIdentifier pluginIdentifier,
            List<Option<?>> requiredOptions,
            List<Option<?>> optionOptions) {
        String TABLE_HEADER =
                "| Name | Type | Required | Default | Description |\n| --- | --- | --- | --- | --- |";

        StringBuilder builder = new StringBuilder();
        builder.append("### ")
                .append(pluginIdentifier.getPluginName())
                .append(StringUtils.SPACE)
                .append(pluginIdentifier.getPluginType())
                .append(StringUtils.SPACE)
                .append(" Options:")
                .append(StringUtils.LF)
                .append(TABLE_HEADER);
        // Required options
        if (!requiredOptions.isEmpty()) {
            builder.append("\n");
            builder.append(
                    getOptionRulesFormatString(
                            requiredOptions, pluginIdentifier.getPluginName(), true));
            builder.deleteCharAt(builder.length() - 1);
        }

        // Optional options
        if (!optionOptions.isEmpty()) {
            builder.append("\n");
            builder.append(
                    getOptionRulesFormatString(
                            optionOptions, pluginIdentifier.getPluginName(), false));
        }

        String result = builder.toString();
        System.out.println(result);
    }

    /**
     * Get the option rules format string.
     *
     * @param options the options
     * @param pluginName the plugin name
     * @param isRequired whether the options are required
     * @return the option rules format string
     */
    private static String getOptionRulesFormatString(
            List<Option<?>> options, String pluginName, boolean isRequired) {
        StringBuilder builder = new StringBuilder();
        for (Option<?> option : options) {
            String key = option.key();
            String type = convert(option.typeReference().getType());
            String requiredOrNot = isRequired ? "true" : "false";
            String defaultValue =
                    option.defaultValue() == null ? "-" : option.defaultValue().toString();
            String description = option.getDescription();
            // Try to get the description from the Chinese i18n config
            try {
                description =
                        SeaTunnelConnectorI18n.CONNECTOR_I18N_CONFIG_ZH
                                .getConfig(pluginName)
                                .getString(key);
            } catch (Exception e) {
                // If not found, use the default English description
            }

            builder.append(
                    String.format(
                            "| %s | %s | %s | %s | %s |\n",
                            key, type, requiredOrNot, defaultValue, description));
        }
        return builder.toString();
    }

    /**
     * Convert a type to a string representation.
     *
     * @param type the type to convert
     * @return the string representation of the type
     */
    private static String convert(Type type) {
        // Handle null case
        if (type == null) {
            return "unknown";
        }

        // Handle parameterized types (generic types)
        if (type instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) type;
            Type rawType = paramType.getRawType();

            Type[] typeArgs = paramType.getActualTypeArguments();

            // Handle List<Map<String, Object>>
            if (rawType.equals(List.class)
                    && typeArgs.length == 1
                    && typeArgs[0] instanceof ParameterizedType) {
                ParameterizedType mapType = (ParameterizedType) typeArgs[0];
                if (mapType.getRawType().equals(Map.class)) {
                    Type[] mapTypeArgs = mapType.getActualTypeArguments();
                    if (mapTypeArgs.length == 2
                            && mapTypeArgs[0].equals(String.class)
                            && mapTypeArgs[1].equals(Object.class)) {
                        return "list_map";
                    }
                }
            }

            if (rawType.equals(List.class)) {
                return "list";
            }
            if (rawType.equals(Map.class)) {
                return "map";
            }
        }

        // Handle specific classes
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;

            // Basic types and wrapper classes
            if (clazz.equals(String.class)) return "string";
            if (clazz.equals(Integer.class) || clazz.equals(int.class)) return "int";
            if (clazz.equals(Long.class) || clazz.equals(long.class)) return "long";
            if (clazz.equals(Double.class) || clazz.equals(double.class)) return "double";
            if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) return "boolean";
            if (clazz.equals(Float.class) || clazz.equals(float.class)) return "float";
            if (clazz.equals(Byte.class) || clazz.equals(byte.class)) return "tinyint";
            if (clazz.equals(Short.class) || clazz.equals(short.class)) return "smallint";

            // Enums and inner classes are displayed as string types
            if (clazz.isEnum() || clazz.getName().contains("$")) {
                return "string";
            }
        }

        // For other unknown types, return the lowercase simple class name.
        if (type instanceof Class) {
            return ((Class<?>) type).getSimpleName().toLowerCase();
        }

        return "unknown";
    }
}
