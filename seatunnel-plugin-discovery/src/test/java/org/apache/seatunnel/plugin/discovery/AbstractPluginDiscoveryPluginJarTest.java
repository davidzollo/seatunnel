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

package org.apache.seatunnel.plugin.discovery;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractPluginDiscoveryPluginJarTest {

    @Test
    public void testAttachPluginJarsBeforeClasspathLookup() throws Exception {
        AtomicReference<List<URL>> capturedPluginJars = new AtomicReference<>();
        AbstractPluginDiscovery<ClasspathPlugin> discovery =
                new AbstractPluginDiscovery<ClasspathPlugin>(
                        Paths.get("target"),
                        ConfigFactory.empty(),
                        (classLoader, urls) -> capturedPluginJars.set(new ArrayList<>(urls))) {
                    @Override
                    protected Class<ClasspathPlugin> getPluginBaseClass() {
                        return ClasspathPlugin.class;
                    }
                };

        URL pluginJarUrl = new URL("file:/tmp/classpath-plugin.jar");

        Optional<ClasspathPlugin> plugin =
                discovery.createOptionalPluginInstance(
                        PluginIdentifier.of("seatunnel", "transform", "ClasspathPlugin"),
                        Collections.singletonList(pluginJarUrl));

        Assertions.assertTrue(plugin.isPresent());
        Assertions.assertEquals(ClasspathPluginProvider.class, plugin.get().getClass());
        Assertions.assertEquals(Collections.singletonList(pluginJarUrl), capturedPluginJars.get());
    }

    @Test
    public void testCreatePluginInstanceWhenClassLoaderCannotBeMutated() throws Exception {
        AbstractPluginDiscovery<ClasspathPlugin> discovery =
                new AbstractPluginDiscovery<ClasspathPlugin>(
                        Paths.get("target"),
                        ConfigFactory.empty(),
                        (classLoader, urls) -> {
                            throw new UnsupportedOperationException(
                                    "Classloader does not support URL injection");
                        }) {
                    @Override
                    protected Class<ClasspathPlugin> getPluginBaseClass() {
                        return ClasspathPlugin.class;
                    }
                };

        URL pluginJarUrl = new URL("file:/tmp/classpath-plugin.jar");

        Optional<ClasspathPlugin> plugin =
                discovery.createOptionalPluginInstance(
                        PluginIdentifier.of("seatunnel", "transform", "ClasspathPlugin"),
                        Collections.singletonList(pluginJarUrl));

        Assertions.assertTrue(plugin.isPresent());
        Assertions.assertEquals(ClasspathPluginProvider.class, plugin.get().getClass());
    }
}
