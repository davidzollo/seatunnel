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
package org.apache.seatunnel.core.starter.seatunnel.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SeaTunnelConnectorI18nTest {
    @Test
    public void testI18n() {
        Config jdbcEn = SeaTunnelConnectorI18n.CONNECTOR_I18N_CONFIG_EN.getConfig("Jdbc");
        Config jdbcZh = SeaTunnelConnectorI18n.CONNECTOR_I18N_CONFIG_ZH.getConfig("Jdbc");

        Assertions.assertEquals(81, jdbcEn.entrySet().size());
        Assertions.assertEquals(81, jdbcZh.entrySet().size());

        String urlZh = jdbcZh.getString("url");
        String urlEn = jdbcEn.getString("url");

        Assertions.assertEquals("JDBC连接Url", urlZh);
        Assertions.assertEquals("JDBC Url", urlEn);
    }
}
