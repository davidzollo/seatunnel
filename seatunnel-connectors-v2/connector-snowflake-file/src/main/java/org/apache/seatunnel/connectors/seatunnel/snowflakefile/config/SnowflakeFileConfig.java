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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@Data
public class SnowflakeFileConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum StagingBackend {
        S3,
        LOCAL_FILE
    }

    public enum LocalStageType {
        USER,
        TABLE,
        NAMED
    }

    // Snowflake connection configuration
    public static final Option<String> ACCOUNT =
            Options.key("account")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake account name");

    public static final Option<String> WAREHOUSE =
            Options.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake warehouse name");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake database name");

    public static final Option<String> SCHEMA =
            Options.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake schema name");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake table name");

    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake user name");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake password");

    public static final Option<String> ROLE =
            Options.key("role")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake role name");

    public static final Option<StagingBackend> STAGING_BACKEND =
            Options.key("staging_backend")
                    .enumType(StagingBackend.class)
                    .defaultValue(StagingBackend.S3)
                    .withDescription("Staging backend type, supported: S3, LOCAL_FILE");

    // S3 configuration
    public static final Option<String> S3_BUCKET =
            Options.key("s3_bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 bucket name for staging files");

    public static final Option<String> S3_PROTOCOL =
            Options.key("s3_protocol")
                    .stringType()
                    .defaultValue("s3")
                    .withDescription("S3 protocol for Snowflake COPY INTO (s3, s3china, etc.)");

    public static final Option<String> S3_REGION =
            Options.key("s3_region")
                    .stringType()
                    .defaultValue("us-east-1")
                    .withDescription("S3 region");

    public static final Option<String> S3_KEY_PREFIX =
            Options.key("s3_key_prefix")
                    .stringType()
                    .defaultValue("snowflake-staging/")
                    .withDescription("S3 key prefix for staging files");

    public static final Option<String> AWS_ACCESS_KEY_ID =
            Options.key("aws_access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS access key ID");

    public static final Option<String> AWS_SECRET_ACCESS_KEY =
            Options.key("aws_secret_access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS secret access key");

    // Local file configuration
    public static final Option<String> LOCAL_TEMP_DIR =
            Options.key("local_temp_dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Local temp directory used before PUT uploads files");

    public static final Option<LocalStageType> LOCAL_STAGE_TYPE =
            Options.key("local_stage_type")
                    .enumType(LocalStageType.class)
                    .defaultValue(LocalStageType.USER)
                    .withDescription("Snowflake internal stage type for local PUT uploads");

    public static final Option<String> LOCAL_STAGE_NAME =
            Options.key("local_stage_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Named internal stage to use when local_stage_type is NAMED");

    public static final Option<String> LOCAL_STAGE_PREFIX =
            Options.key("local_stage_prefix")
                    .stringType()
                    .defaultValue("seatunnel-local")
                    .withDescription("Prefix used in the Snowflake internal stage path");

    // File format configuration
    public static final Option<String> FILE_FORMAT =
            Options.key("file_format")
                    .stringType()
                    .defaultValue("CSV")
                    .withDescription("File format (CSV, JSON, PARQUET, AVRO, ORC)");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter for CSV files");

    public static final Option<String> RECORD_DELIMITER =
            Options.key("record_delimiter")
                    .stringType()
                    .defaultValue("\\n")
                    .withDescription("Record delimiter for CSV files");

    public static final Option<String> FILE_EXTENSION =
            Options.key("file_extension")
                    .stringType()
                    .defaultValue(".csv")
                    .withDescription("File extension");

    // Performance configuration
    public static final Option<Integer> BUFFER_SIZE =
            Options.key("buffer_size")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription("Buffer size in bytes (default: 1MB)");

    public static final Option<Long> MAX_FILE_SIZE =
            Options.key("max_file_size")
                    .longType()
                    .defaultValue(1048576L)
                    .withDescription("Maximum file size in bytes (default: 1MB)");

    // COPY options
    public static final Option<Map<String, String>> COPY_OPTIONS =
            Options.key("copy_options")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional COPY INTO options");

    public static final Option<Boolean> PURGE_AFTER_COPY =
            Options.key("purge_after_copy")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to purge files after successful COPY");

    // Time format configuration
    public static final Option<String> TIME_FORMAT =
            Options.key("time_format")
                    .stringType()
                    .defaultValue("HH24:MI:SS")
                    .withDescription("Time format for Snowflake");

    public static final Option<String> DATE_FORMAT =
            Options.key("date_format")
                    .stringType()
                    .defaultValue("YYYY-MM-DD")
                    .withDescription("Date format for Snowflake");

    public static final Option<String> TIMESTAMP_FORMAT =
            Options.key("timestamp_format")
                    .stringType()
                    .defaultValue("YYYY-MM-DD HH24:MI:SS.FF3")
                    .withDescription("Timestamp format for Snowflake");

    public static final Option<String> SNOWFLAKE_FILE_FORMAT_NAME =
            Options.key("snowflake_file_format_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Existing Snowflake file format name");

    // Save mode configuration
    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("Schema save mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("Data save mode");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom SQL for save mode");

    // Instance fields
    private String account;
    private String warehouse;
    private String database;
    private String schema;
    private String table;
    private String user;
    private String password;
    private String role;

    private StagingBackend stagingBackend;

    // S3 config
    private String s3Bucket;
    private String s3Protocol;
    private String s3Region;
    private String s3KeyPrefix;
    private String awsAccessKeyId;
    private String awsSecretAccessKey;

    // Local file config
    private String localTempDir;
    private LocalStageType localStageType;
    private String localStageName;
    private String localStagePrefix;

    // File format config
    private String fileFormat;
    private String fieldDelimiter;
    private String recordDelimiter;
    private String fileExtension;

    // Performance config
    private int bufferSize;
    private long maxFileSize;

    // COPY options
    private Map<String, String> copyOptions;
    private boolean purgeAfterCopy;

    // Time format
    private String timeFormat;
    private String dateFormat;
    private String timestampFormat;

    // Snowflake file format
    private String snowflakeFileFormatName;

    // Save mode config
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private String customSql;

    // Runtime config
    private SeaTunnelRowType seaTunnelRowType;

    public SnowflakeFileConfig(Config config) {
        // Snowflake connection config
        this.account = config.getString("account");
        this.warehouse = getOptionalString(config, "warehouse");
        this.database = config.getString("database");
        this.schema = config.getString("schema");
        this.table = config.getString("table");
        this.user = config.getString("user");
        this.password = config.getString("password");
        this.role = config.hasPath("role") ? config.getString("role") : null;
        this.stagingBackend =
                config.hasPath("staging_backend")
                        ? StagingBackend.valueOf(config.getString("staging_backend"))
                        : StagingBackend.S3;

        // S3 config
        if (stagingBackend == StagingBackend.S3) {
            this.s3Bucket = config.getString("s3_bucket");
            this.s3Protocol =
                    config.hasPath("s3_protocol") ? config.getString("s3_protocol") : "s3";
            this.s3Region = config.getString("s3_region");
            this.s3KeyPrefix = config.getString("s3_key_prefix");
            this.awsAccessKeyId = config.getString("aws_access_key_id");
            this.awsSecretAccessKey = config.getString("aws_secret_access_key");
        } else {
            this.s3Bucket = null;
            this.s3Protocol = null;
            this.s3Region = null;
            this.s3KeyPrefix = null;
            this.awsAccessKeyId = null;
            this.awsSecretAccessKey = null;
        }

        // Local file config
        this.localTempDir =
                config.hasPath("local_temp_dir")
                        ? config.getString("local_temp_dir")
                        : System.getProperty("java.io.tmpdir") + "/seatunnel-snowflake-file";
        this.localStageType =
                config.hasPath("local_stage_type")
                        ? LocalStageType.valueOf(config.getString("local_stage_type"))
                        : LocalStageType.USER;
        this.localStageName =
                config.hasPath("local_stage_name") ? config.getString("local_stage_name") : null;
        this.localStagePrefix =
                config.hasPath("local_stage_prefix")
                        ? config.getString("local_stage_prefix")
                        : "seatunnel-local";

        // File format config
        this.fileFormat = config.getString("file_format");
        this.fieldDelimiter = config.getString("field_delimiter");
        this.recordDelimiter = config.getString("record_delimiter");
        this.fileExtension = config.getString("file_extension");

        // Performance config
        this.bufferSize = config.getInt("buffer_size");
        this.maxFileSize = config.getLong("max_file_size");

        // COPY options
        Map<String, Object> copyOptionsRaw =
                config.hasPath("copy_options")
                        ? config.getObject("copy_options").unwrapped()
                        : Collections.emptyMap();
        this.copyOptions = new java.util.HashMap<>();
        copyOptionsRaw.forEach((k, v) -> this.copyOptions.put(k, String.valueOf(v)));
        this.purgeAfterCopy = config.getBoolean("purge_after_copy");

        // Time format
        this.timeFormat = config.getString("time_format");
        this.dateFormat = config.getString("date_format");
        this.timestampFormat = config.getString("timestamp_format");

        // Snowflake file format
        this.snowflakeFileFormatName =
                config.hasPath("snowflake_file_format_name")
                        ? config.getString("snowflake_file_format_name")
                        : null;

        // Save mode config
        this.schemaSaveMode =
                config.hasPath("schema_save_mode")
                        ? SchemaSaveMode.valueOf(config.getString("schema_save_mode"))
                        : SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST;
        this.dataSaveMode =
                config.hasPath("data_save_mode")
                        ? DataSaveMode.valueOf(config.getString("data_save_mode"))
                        : DataSaveMode.APPEND_DATA;
        this.customSql = config.hasPath("custom_sql") ? config.getString("custom_sql") : null;
    }

    private String getOptionalString(Config config, String path) {
        if (!config.hasPath(path)) {
            return null;
        }
        String value = config.getString(path);
        return value == null || value.trim().isEmpty() ? null : value;
    }

    // Getters for all fields
    public String getAccount() {
        return account;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }

    public StagingBackend getStagingBackend() {
        return stagingBackend;
    }

    public boolean isS3StagingBackend() {
        return stagingBackend == StagingBackend.S3;
    }

    public boolean isLocalFileStagingBackend() {
        return stagingBackend == StagingBackend.LOCAL_FILE;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Protocol() {
        return s3Protocol;
    }

    public String getS3Region() {
        return s3Region;
    }

    public String getS3KeyPrefix() {
        return s3KeyPrefix;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public String getAwsSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public String getLocalTempDir() {
        return localTempDir;
    }

    public LocalStageType getLocalStageType() {
        return localStageType;
    }

    public String getLocalStageName() {
        return localStageName;
    }

    public String getLocalStagePrefix() {
        return localStagePrefix;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public String getRecordDelimiter() {
        return recordDelimiter;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public Map<String, String> getCopyOptions() {
        return copyOptions;
    }

    public boolean isPurgeAfterCopy() {
        return purgeAfterCopy;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public String getSnowflakeFileFormatName() {
        return snowflakeFileFormatName;
    }

    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }

    public DataSaveMode getDataSaveMode() {
        return dataSaveMode;
    }

    public String getCustomSql() {
        return customSql;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }

    public void setSeaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }
}
