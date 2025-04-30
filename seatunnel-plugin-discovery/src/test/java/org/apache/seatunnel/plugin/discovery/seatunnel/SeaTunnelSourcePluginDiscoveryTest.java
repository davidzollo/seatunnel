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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnOs(OS.WINDOWS)
class SeaTunnelSourcePluginDiscoveryTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private static final String seatunnelHome =
            SeaTunnelSourcePluginDiscoveryTest.class
                    .getResource("/SeaTunnelSourcePluginDiscoveryTest")
                    .getPath();
    private static final List<Path> pluginJars =
            Lists.newArrayList(
                    Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-http.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-jdbc.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-clickhouse.jar"),
                    Paths.get(
                            seatunnelHome,
                            "plugins",
                            "connector-clickhouse",
                            "clickhouse-jdbc-driver.jar"),
                    Paths.get(
                            seatunnelHome,
                            "plugins",
                            "connector-clickhouse",
                            "clickhouse-jdbc-driver2.jar"),
                    Paths.get(seatunnelHome, "plugins", "connector-jdbc", "mysql-jdbc-driver.jar"),
                    Paths.get(seatunnelHome, "plugins", "connector-jdbc", "mysql-jdbc-driver2.jar"),
                    Paths.get(seatunnelHome, "plugins", "other", "common-dependency.jar"),
                    Paths.get(seatunnelHome, "plugins", "other", "common-dependency2.jar"),
                    Paths.get(seatunnelHome, "plugins", "common-dependency3.jar"),
                    Paths.get(
                            seatunnelHome,
                            "plugins",
                            "otherWithLib",
                            "lib",
                            "common-dependency3.jar"));

    @BeforeEach
    public void before() throws IOException {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);

        // The file is created under target directory.
        for (Path pluginJar : pluginJars) {
            FileUtils.createNewFile(pluginJar.toString());
        }
    }

    @Test
    void getPluginBaseClass() {
        List<PluginIdentifier> pluginIdentifiers =
                Lists.newArrayList(
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpJira"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpBase"));
        SeaTunnelSourcePluginDiscovery seaTunnelSourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery();

        Assertions.assertIterableEquals(
                Stream.of(
                                Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-http.jar")
                                        .toString())
                        .collect(Collectors.toList()),
                seaTunnelSourcePluginDiscovery.getPluginJarPaths(pluginIdentifiers).stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testGetPluginDependencies() throws MalformedURLException {
        PluginIdentifier jdbc =
                PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "JDBC");
        PluginIdentifier clickhouse =
                PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "ClickHouse");
        SeaTunnelSourcePluginDiscovery discovery = new SeaTunnelSourcePluginDiscovery();
        List<URL> jdbcAndClickHouseJars =
                discovery.getPluginJarAndDependencyPaths(Lists.newArrayList(jdbc, clickhouse));
        Assertions.assertIterableEquals(
                Lists.newArrayList(
                        new URL("file:" + seatunnelHome + "/connectors/connector-clickhouse.jar"),
                        new URL("file:" + seatunnelHome + "/connectors/connector-jdbc.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/common-dependency3.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-clickhouse/clickhouse-jdbc-driver.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-clickhouse/clickhouse-jdbc-driver2.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-jdbc/mysql-jdbc-driver.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-jdbc/mysql-jdbc-driver2.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency2.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/otherWithLib/lib/common-dependency3.jar")),
                jdbcAndClickHouseJars);
        List<URL> jdbcJars = discovery.getPluginJarAndDependencyPaths(Lists.newArrayList(jdbc));
        Assertions.assertIterableEquals(
                Lists.newArrayList(
                        new URL("file:" + seatunnelHome + "/connectors/connector-jdbc.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/common-dependency3.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-jdbc/mysql-jdbc-driver.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-jdbc/mysql-jdbc-driver2.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency2.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/otherWithLib/lib/common-dependency3.jar")),
                jdbcJars);
        List<URL> clickhouseJars =
                discovery.getPluginJarAndDependencyPaths(Lists.newArrayList(clickhouse));
        Assertions.assertIterableEquals(
                Lists.newArrayList(
                        new URL("file:" + seatunnelHome + "/connectors/connector-clickhouse.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/common-dependency3.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-clickhouse/clickhouse-jdbc-driver.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/connector-clickhouse/clickhouse-jdbc-driver2.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency.jar"),
                        new URL("file:" + seatunnelHome + "/plugins/other/common-dependency2.jar"),
                        new URL(
                                "file:"
                                        + seatunnelHome
                                        + "/plugins/otherWithLib/lib/common-dependency3.jar")),
                clickhouseJars);
    }

    @Test
    public void testGetPluginsJarDependenciesWithoutConnectorDependency()
            throws MalformedURLException, URISyntaxException {
        List<Path> paths = Common.getPluginsJarDependenciesWithoutConnectorDependency();
        Assertions.assertIterableEquals(
                Collections.singletonList(
                        Paths.get(
                                new URL(
                                                "file:"
                                                        + seatunnelHome
                                                        + "/plugins/otherWithLib/lib/common-dependency3.jar")
                                        .toURI())),
                paths);
    }

    @AfterEach
    public void after() throws IOException {
        for (Path pluginJar : pluginJars) {
            Files.deleteIfExists(pluginJar);
        }
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
    }
}
