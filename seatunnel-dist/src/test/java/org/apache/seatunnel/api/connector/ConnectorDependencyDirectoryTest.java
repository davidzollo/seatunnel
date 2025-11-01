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

package org.apache.seatunnel.api.connector;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Disabled("Temporarily disabled - needs to be fixed")
public class ConnectorDependencyDirectoryTest {

    @Test
    void testAllDirectoryAreExisted() throws URISyntaxException, IOException {
        Path root =
                Paths.get(
                                ConnectorDependencyDirectoryTest.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        .getParent()
                        .getParent()
                        .getParent();
        Path pluginMapping = Paths.get(root.toString(), "plugin-mapping.properties");
        Properties properties = new Properties();
        properties.load(Files.newInputStream(pluginMapping));
        properties.values().stream()
                .distinct()
                .forEach(
                        path -> {
                            File file =
                                    Paths.get(
                                                    root.toString(),
                                                    "seatunnel-dist/src/main/resources/" + path)
                                            .toFile();
                            if (!file.exists() || !file.isDirectory()) {
                                throw new RuntimeException(
                                        "Directory not existed: "
                                                + file.getAbsolutePath()
                                                + ", please add create connector directory task in createConnectorDependencyDirectory plugin execution of pom.xml.");
                            }
                        });
    }
}
