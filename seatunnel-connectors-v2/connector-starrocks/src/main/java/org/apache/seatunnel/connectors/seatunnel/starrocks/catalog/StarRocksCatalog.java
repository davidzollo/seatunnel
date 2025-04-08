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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.starrocks.JdbcStarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StarRocksCatalog extends JdbcStarRocksCatalog {

    protected final String catalogName;
    protected String defaultDatabase = "information_schema";
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected String defaultUrl;
    private final JdbcUrlUtil.UrlInfo urlInfo;
    private final String template;
    private Connection conn;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    public StarRocksCatalog(
            String catalogName, String username, String pwd, String defaultUrl, String template) {

        super(catalogName, username, pwd, JdbcUrlUtil.getUrlInfo(defaultUrl));
        this.urlInfo = JdbcUrlUtil.getUrlInfo(defaultUrl);
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        if (urlInfo.getDefaultDatabase().isPresent()) {
            this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        }
        this.defaultUrl = defaultUrl;
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
        this.template = template;
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select * from %s limit 1", tablePath.getFullName());
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return StarRocksSaveModeUtil.getCreateTableSql(
                template,
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                table.getTableSchema(),
                table.getComment());
    }

    @SuppressWarnings("MagicNumber")
    protected Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "starrocks");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public String name() {
        return catalogName;
    }
}
