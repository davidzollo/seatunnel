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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SnowflakeFileSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "SnowflakeFile";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // Required options
                .required(
                        SnowflakeFileConfig.ACCOUNT,
                        SnowflakeFileConfig.DATABASE,
                        SnowflakeFileConfig.SCHEMA,
                        SnowflakeFileConfig.TABLE,
                        SnowflakeFileConfig.USER,
                        SnowflakeFileConfig.PASSWORD,
                        SnowflakeFileConfig.STAGING_BACKEND)
                // Optional options
                .optional(
                        SnowflakeFileConfig.S3_REGION,
                        SnowflakeFileConfig.S3_KEY_PREFIX,
                        SnowflakeFileConfig.S3_PROTOCOL,
                        SnowflakeFileConfig.LOCAL_TEMP_DIR,
                        SnowflakeFileConfig.LOCAL_STAGE_TYPE,
                        SnowflakeFileConfig.LOCAL_STAGE_PREFIX,
                        SnowflakeFileConfig.WAREHOUSE,
                        SnowflakeFileConfig.ROLE,
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
                .conditional(
                        SnowflakeFileConfig.STAGING_BACKEND,
                        SnowflakeFileConfig.StagingBackend.S3,
                        SnowflakeFileConfig.S3_BUCKET,
                        SnowflakeFileConfig.AWS_ACCESS_KEY_ID,
                        SnowflakeFileConfig.AWS_SECRET_ACCESS_KEY)
                .conditional(
                        SnowflakeFileConfig.LOCAL_STAGE_TYPE,
                        SnowflakeFileConfig.LocalStageType.NAMED,
                        SnowflakeFileConfig.LOCAL_STAGE_NAME)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> {
            SnowflakeFileSink sink = new SnowflakeFileSink();
            sink.prepare(context.getOptions().toConfig());
            sink.setTypeInfo(context.getCatalogTable().getTableSchema().toPhysicalRowDataType());
            return sink;
        };
    }
}
