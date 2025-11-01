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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoService(Factory.class)
public class SnowflakeCatalogFactory implements CatalogFactory {

    @VisibleForTesting
    public static final Pattern URL_PATTERN =
            Pattern.compile("^(?<url>jdbc:snowflake://(?<host>.+?))(?<suffix>/.*)$");

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.SNOWFLAKE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        String database = options.get(JdbcOptions.DATABASE);
        JdbcUrlUtil.UrlInfo urlInfo = parseSnowflakeUrl(urlWithDatabase, database);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(JdbcCatalogOptions.BASE_URL);
        }
        return new SnowflakeCatalog(
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

    private JdbcUrlUtil.UrlInfo parseSnowflakeUrl(String url, String database) {
        Matcher matcher = URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String urlWithoutDatabase = matcher.group("url");
            String host = matcher.group("host");
            String suffix = matcher.group("suffix");
            return new JdbcUrlUtil.UrlInfo(
                    url,
                    urlWithoutDatabase,
                    host,
                    null, // port is not applicable for Snowflake
                    database,
                    suffix);
        }
        throw new IllegalArgumentException("The Snowflake jdbc url format is incorrect: " + url);
    }
}
