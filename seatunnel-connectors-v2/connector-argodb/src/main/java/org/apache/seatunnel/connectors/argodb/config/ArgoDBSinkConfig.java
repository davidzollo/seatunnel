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

package org.apache.seatunnel.connectors.argodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Builder
@ToString
@Getter
public class ArgoDBSinkConfig implements Serializable {
    public static final String PLUGIN_IDENTIFIER = "ArgoDB";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL of the ArgoDB service");
    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username to use to connect to the ArgoDB service");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password to use to connect to the ArgoDB service");
    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The database to use in the ArgoDB service");
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table to use in the ArgoDB service");
    public static final Option<String> TMP_DIRECTORY =
            Options.key("tmp_dir")
                    .stringType()
                    .defaultValue("/tmp")
                    .withDescription("The directory to use for temporary files");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(10240)
                    .withDescription("The number of records to batch before sending to ArgoDB");
    public static final Option<Boolean> ENABLE_UPSERT_DELETE =
            Options.key("enable_upsert_delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable upsert and delete operations in ArgoDB");

    private final String url;
    private final String user;
    private final String password;
    private final String database;
    private final String table;
    private final String tmpDirectory;
    private final int batchSize;
    private final boolean enableUpsertDelete;

    public String getTablePath() {
        return database + "." + table;
    }

    public static ArgoDBSinkConfig fromConfig(ReadonlyConfig config) {
        return ArgoDBSinkConfig.builder()
                .url(config.get(URL))
                .user(config.get(USER))
                .password(config.get(PASSWORD))
                .database(config.get(DATABASE))
                .table(config.get(TABLE))
                .tmpDirectory(config.get(TMP_DIRECTORY))
                .batchSize(config.get(BATCH_SIZE))
                .enableUpsertDelete(config.get(ENABLE_UPSERT_DELETE))
                .build();
    }
}
