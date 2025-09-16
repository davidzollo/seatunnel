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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DamengCatalogFactoryTest {

    @Test
    public void testDMClusterUrlParsing() {
        DamengCatalogFactory factory = new DamengCatalogFactory();

        // Test DM cluster URL with DM= parameter
        Map<String, Object> config1 = new HashMap<>();
        config1.put(
                JdbcCatalogOptions.BASE_URL.key(),
                "jdbc:dm://DM?DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)");
        config1.put(JdbcCatalogOptions.USERNAME.key(), "test");
        config1.put(JdbcCatalogOptions.PASSWORD.key(), "test");

        ReadonlyConfig readonlyConfig1 = ReadonlyConfig.fromMap(config1);
        Catalog catalog1 = factory.createCatalog("test", readonlyConfig1);

        Assertions.assertNotNull(catalog1);
        Assertions.assertTrue(catalog1 instanceof DamengCatalog);

        // Test DM cluster URL with DMA= parameter
        Map<String, Object> config2 = new HashMap<>();
        config2.put(
                JdbcCatalogOptions.BASE_URL.key(),
                "jdbc:dm://DMA?DMA=(datasource01:5236)&schema=SYSDBA&localTimezone=480&language=CN&loginMode=0");
        config2.put(JdbcCatalogOptions.USERNAME.key(), "test");
        config2.put(JdbcCatalogOptions.PASSWORD.key(), "test");

        ReadonlyConfig readonlyConfig2 = ReadonlyConfig.fromMap(config2);
        Catalog catalog2 = factory.createCatalog("test", readonlyConfig2);

        Assertions.assertNotNull(catalog2);
        Assertions.assertTrue(catalog2 instanceof DamengCatalog);

        // Test DM cluster URL with database and DMB= parameter
        Map<String, Object> config3 = new HashMap<>();
        config3.put(
                JdbcCatalogOptions.BASE_URL.key(),
                "jdbc:dm://DMB/testdb?DMB=(host1:5236,host2:5236)&schema=TEST");
        config3.put(JdbcCatalogOptions.USERNAME.key(), "test");
        config3.put(JdbcCatalogOptions.PASSWORD.key(), "test");

        ReadonlyConfig readonlyConfig3 = ReadonlyConfig.fromMap(config3);
        Catalog catalog3 = factory.createCatalog("test", readonlyConfig3);

        Assertions.assertNotNull(catalog3);
        Assertions.assertTrue(catalog3 instanceof DamengCatalog);
    }

    @Test
    public void testFactoryIdentifier() {
        DamengCatalogFactory factory = new DamengCatalogFactory();
        Assertions.assertEquals("dameng", factory.factoryIdentifier());
    }

    @Test
    public void testOptionRule() {
        DamengCatalogFactory factory = new DamengCatalogFactory();
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    public void testGetUrlInfo() {
        // Test DM cluster URL - should preserve cluster parameter in urlWithoutDatabase
        String url = "jdbc:dm://DM?DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)";
        JdbcUrlUtil.UrlInfo urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals(
                "jdbc:dm://DM?DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)",
                urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("10.63.97.4", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals(
                "?DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)", urlInfo.getSuffix());

        url = "jdbc:dm://10.63.97.4:5236/testdb";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals("jdbc:dm://10.63.97.4:5236", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("10.63.97.4", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("testdb", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals("", urlInfo.getSuffix());

        url = "jdbc:dm://10.63.97.4:5236/testdb?schema=TEST";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals("jdbc:dm://10.63.97.4:5236", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("10.63.97.4", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("testdb", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals("?schema=TEST", urlInfo.getSuffix());

        url =
                "jdbc:dm://10.63.97.4:5236/testdb?schema=TEST&localTimezone=480&language=CN&loginMode=0";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals("jdbc:dm://10.63.97.4:5236", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("10.63.97.4", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("testdb", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals(
                "?schema=TEST&localTimezone=480&language=CN&loginMode=0", urlInfo.getSuffix());

        url =
                "jdbc:dm://10.63.97.4:5236/testdb?schema=TEST&localTimezone=480&language=CN&loginMode=0&DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals("jdbc:dm://10.63.97.4:5236", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("10.63.97.4", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("testdb", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals(
                "?schema=TEST&localTimezone=480&language=CN&loginMode=0&DM=(10.63.97.4:5236,10.63.97.5:5236,10.63.97.7:5236)",
                urlInfo.getSuffix());

        // Test DMA cluster URL with additional parameters - should only preserve cluster parameter
        url =
                "jdbc:dm://DMA?DMA=(datasource01:5236)&schema=SYSDBA&localTimezone=480&language=CN&loginMode=0";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals(
                "jdbc:dm://DMA?DMA=(datasource01:5236)", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("datasource01", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals(
                "?DMA=(datasource01:5236)&schema=SYSDBA&localTimezone=480&language=CN&loginMode=0",
                urlInfo.getSuffix());

        // Test DMB cluster URL with database - should only preserve cluster parameter
        url = "jdbc:dm://DMB/testdb?DMB=(host1:5236,host2:5236)&schema=TEST";
        urlInfo = DamengCatalogFactory.DM_URL_PARSER.apply(url);
        Assertions.assertEquals(
                "jdbc:dm://DMB?DMB=(host1:5236,host2:5236)", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("host1", urlInfo.getHost());
        Assertions.assertEquals(5236, urlInfo.getPort().intValue());
        Assertions.assertEquals("testdb", urlInfo.getDefaultDatabase().orElse(""));
        Assertions.assertEquals("?DMB=(host1:5236,host2:5236)&schema=TEST", urlInfo.getSuffix());
    }
}
