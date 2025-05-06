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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.BaseHiveTest;

import org.apache.hadoop.hive.metastore.api.Table;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;

class HiveMetaStoreProxyTest extends BaseHiveTest {

    @Disabled
    @Test
    void getTable() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/hive.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        try (HiveMetaStoreProxy hiveMetaStoreProxy = new HiveMetaStoreProxy(readonlyConfig)) {
            Table table = hiveMetaStoreProxy.getTable("default", "czjtest_03");
            System.out.println(table);
        }
    }
}
