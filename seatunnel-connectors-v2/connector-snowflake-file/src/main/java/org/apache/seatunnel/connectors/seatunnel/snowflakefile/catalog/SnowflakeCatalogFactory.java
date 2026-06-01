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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SnowflakeCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return "SnowflakeFile";
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        // Create SnowflakeFile config
        SnowflakeFileConfig config = new SnowflakeFileConfig(options.toConfig());

        // Create and return SnowflakeFileCatalog
        return new org.apache.seatunnel.connectors.seatunnel.snowflakefile.catalog
                .SnowflakeFileCatalog(config);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // Required options
                .required(
                        SnowflakeFileConfig.ACCOUNT,
                        SnowflakeFileConfig.DATABASE,
                        SnowflakeFileConfig.SCHEMA,
                        SnowflakeFileConfig.USER,
                        SnowflakeFileConfig.PASSWORD)
                // Optional options
                .optional(
                        SnowflakeFileConfig.WAREHOUSE,
                        SnowflakeFileConfig.ROLE,
                        SnowflakeFileConfig.S3_BUCKET,
                        SnowflakeFileConfig.S3_REGION,
                        SnowflakeFileConfig.S3_KEY_PREFIX,
                        SnowflakeFileConfig.FILE_FORMAT,
                        SnowflakeFileConfig.FIELD_DELIMITER,
                        SnowflakeFileConfig.RECORD_DELIMITER,
                        SnowflakeFileConfig.FILE_EXTENSION,
                        SnowflakeFileConfig.BUFFER_SIZE,
                        SnowflakeFileConfig.MAX_FILE_SIZE,
                        SnowflakeFileConfig.PURGE_AFTER_COPY,
                        SnowflakeFileConfig.TIME_FORMAT,
                        SnowflakeFileConfig.DATE_FORMAT,
                        SnowflakeFileConfig.TIMESTAMP_FORMAT,
                        SnowflakeFileConfig.SNOWFLAKE_FILE_FORMAT_NAME,
                        SnowflakeFileConfig.COPY_OPTIONS,
                        SnowflakeFileConfig.SCHEMA_SAVE_MODE,
                        SnowflakeFileConfig.DATA_SAVE_MODE,
                        SnowflakeFileConfig.CUSTOM_SQL)
                .build();
    }
}
