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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.gbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;
import java.util.regex.Pattern;

@AutoService(Factory.class)
public class Gbase8sCatalogFactory implements CatalogFactory {

    @VisibleForTesting
    public static final Pattern URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:.+?//(?<host>.+?):(?<port>\\d+?))(/(?<database>.*?))*(?<suffix>:.*)*$");

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.GBASE_8S;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase, URL_PATTERN);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(JdbcCatalogOptions.BASE_URL);
        }
        String compatibleMode = options.get(JdbcCatalogOptions.COMPATIBLE_MODE);
        if (compatibleMode != null && "informix".equalsIgnoreCase(compatibleMode)) {
            return new Gbase8sInformixCatalog(
                    catalogName,
                    options.get(JdbcCatalogOptions.USERNAME),
                    options.get(JdbcCatalogOptions.PASSWORD),
                    urlInfo,
                    options.get(JdbcCatalogOptions.SCHEMA));
        }
        return new Gbase8sCatalog(
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
