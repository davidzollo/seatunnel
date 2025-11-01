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
package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.server.license.LicenseDelegator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.whaleops.license.dto.LicenseInfo;
import org.whaleops.license.enums.LicenseNodeEnum;
import org.whaleops.license.utils.LicenseUtil;

import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;

@Disabled("Temporarily disabled - needs to be fixed")
public class LicenseTest {

    @Test
    @SneakyThrows
    public void TestLicenseCheck() {
        System.setProperty(
                "SEATUNNEL_LICENCE_HOME", getTestConfigFile("/license/whaletunnel.license"));

        LicenseDelegator licenseDelegator = new LicenseDelegator(getEngineConfig());
        final LicenseInfo latestValidLicenseInfo = licenseDelegator.getSystemLicense();
        Assertions.assertTrue(LicenseUtil.checkLicenseStartAndEndTime(latestValidLicenseInfo));
        Assertions.assertTrue(
                LicenseUtil.checkLicenseServer(
                        latestValidLicenseInfo,
                        new HashSet<>(),
                        "172.18.22.204",
                        LicenseNodeEnum.WT));
        Assertions.assertTrue(
                LicenseUtil.checkLicenseServer(
                        latestValidLicenseInfo,
                        new HashSet<>(),
                        "172.18.22.207",
                        LicenseNodeEnum.WT));
        Assertions.assertTrue(
                LicenseUtil.checkLicenseServer(
                        latestValidLicenseInfo, new HashSet<>(), "127.0.0.1", LicenseNodeEnum.WT));
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = LicenseTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    private static EngineConfig getEngineConfig() {
        return new EngineConfig();
    }
}
