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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoService(Factory.class)
public class DamengCatalogFactory implements CatalogFactory {

    private static final Pattern DM_URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:(?<protocol>\\w+)://(?<hostpart>[^/]+?))(/(?<database>.*?))*(?<suffix>\\?.*)*$");

    private static final Pattern CLUSTER_PATTERN = Pattern.compile("[?&]([A-Z]+)=\\(([^)]+)\\)");

    @VisibleForTesting
    public static final Function<String, JdbcUrlUtil.UrlInfo> DM_URL_PARSER =
            url -> {
                try {
                    return JdbcUrlUtil.getUrlInfo(url);
                } catch (IllegalArgumentException illegalArgumentException) {
                    Matcher matcher = DM_URL_PATTERN.matcher(url);
                    if (matcher.find()) {
                        String urlWithoutDatabase = matcher.group("url");
                        String database = matcher.group("database");
                        String hostpart = matcher.group("hostpart");
                        String protocol = matcher.group("protocol");
                        String suffix = matcher.group("suffix");

                        String host = hostpart;
                        Integer port = 0;

                        if ("dm".equalsIgnoreCase(protocol) && suffix != null) {
                            Matcher clusterMatcher = CLUSTER_PATTERN.matcher(suffix);

                            if (clusterMatcher.find()) {
                                String paramName = clusterMatcher.group(1);
                                String hostList = clusterMatcher.group(2);
                                String clusterParam = paramName + "=(" + hostList + ")";

                                String[] hosts = hostList.split(",");
                                if (hosts.length > 0) {
                                    String firstHost = hosts[0].trim();
                                    String[] hostPortPair = firstHost.split(":");
                                    if (hostPortPair.length == 2) {
                                        host = hostPortPair[0];
                                        try {
                                            port = Integer.valueOf(hostPortPair[1]);
                                            urlWithoutDatabase =
                                                    "jdbc:"
                                                            + protocol
                                                            + "://"
                                                            + hostpart
                                                            + "?"
                                                            + clusterParam;
                                        } catch (NumberFormatException e) {
                                            port = 0;
                                        }
                                    }
                                }
                            }
                        }
                        return new JdbcUrlUtil.UrlInfo(
                                url, urlWithoutDatabase, host, port, database, suffix);
                    }
                }
                throw new IllegalArgumentException("The DM jdbc url format is incorrect: " + url);
            };

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        Preconditions.checkArgument(
                StringUtils.isNotBlank(urlWithDatabase),
                "Miss config <base-url>! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase, DM_URL_PARSER);
        return new DamengCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}
