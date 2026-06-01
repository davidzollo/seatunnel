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

package org.apache.seatunnel.connectors.seatunnel.snowflakefile.sink.client;

import org.apache.seatunnel.connectors.seatunnel.snowflakefile.config.SnowflakeFileConfig;
import org.apache.seatunnel.connectors.seatunnel.snowflakefile.util.SnowflakeIdentifierUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class SnowflakeCopyIntoClient {

    private final SnowflakeFileConfig config;
    private Connection connection;
    private String fileFormatName;
    private final String localStageLocation;

    public SnowflakeCopyIntoClient(SnowflakeFileConfig config) {
        this.config = config;
        this.fileFormatName = config.getSnowflakeFileFormatName();
        this.localStageLocation = initializeLocalStageLocation();
    }

    /** Establish the Snowflake connection */
    public void connect() throws SQLException {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Snowflake JDBC driver not found", e);
        }

        String jdbcUrl = buildJdbcUrl();
        Properties properties = new Properties();
        properties.put("user", config.getUser());
        properties.put("password", config.getPassword());
        properties.put("db", config.getDatabase());
        properties.put("schema", config.getSchema());
        properties.put("enablePutGet", "true");

        if (hasText(config.getWarehouse())) {
            properties.put("warehouse", config.getWarehouse());
        }

        if (config.getRole() != null) {
            properties.put("role", config.getRole());
        }

        this.connection = DriverManager.getConnection(jdbcUrl, properties);
        initializeSessionContext();
        log.info(
                "Connected to Snowflake: {}/{}/{}",
                config.getAccount(),
                config.getDatabase(),
                config.getSchema());
    }

    /** Build JDBC URL */
    private String buildJdbcUrl() {
        return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", config.getAccount());
    }

    void initializeSessionContext() throws SQLException {
        if (hasText(config.getWarehouse())) {
            useSessionObject("WAREHOUSE", config.getWarehouse());
        }
        useSessionObject("DATABASE", config.getDatabase());
        useSessionObject("SCHEMA", config.getSchema());
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private void useSessionObject(String objectType, String objectName) throws SQLException {
        if (objectName == null || objectName.trim().isEmpty()) {
            return;
        }
        String sql = "USE " + objectType + " " + quoteIdentifier(objectName.trim());
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("Initialized Snowflake session with {}", sql);
        }
    }

    private String quoteIdentifier(String identifier) {
        return SnowflakeIdentifierUtils.quoteIdentifier(identifier);
    }

    /** Create the file format if absent */
    public void createFileFormatIfNotExists() throws SQLException {
        if (fileFormatName != null) {
            // Use the existing file format
            log.info("Using existing file format: {}", fileFormatName);
            return;
        }

        // Create a temporary file format
        this.fileFormatName = "SEATUNNEL_TEMP_FORMAT_" + System.currentTimeMillis();

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE FILE FORMAT IF NOT EXISTS ").append(fileFormatName).append(" ");
        sql.append("TYPE = '").append(config.getFileFormat()).append("' ");

        switch (config.getFileFormat()) {
            case "CSV":
                sql.append("FIELD_DELIMITER = '").append(config.getFieldDelimiter()).append("' ");
                sql.append("RECORD_DELIMITER = '")
                        .append(config.getRecordDelimiter().replace("\n", "\\n"))
                        .append("' ");
                sql.append("SKIP_HEADER = 0 ");
                sql.append("FIELD_OPTIONALLY_ENCLOSED_BY = '\\\"' ");
                sql.append("NULL_IF = ('') ");
                break;
            case "JSON":
                sql.append("STRIP_OUTER_ARRAY = TRUE ");
                break;
            case "PARQUET":
                // Use defaults for Parquet format
                break;
        }

        try (Statement stmt = connection.createStatement()) {
            log.info("Creating file format: {}", sql.toString());
            stmt.execute(sql.toString());
        }
    }

    /** Execute COPY INTO command */
    public void copyIntoTable(List<String> fileUrls) throws SQLException {
        if (fileUrls == null || fileUrls.isEmpty()) {
            log.warn("No S3 files to copy");
            return;
        }

        if (config.isLocalFileStagingBackend()) {
            copyIntoTableFromLocalFiles(fileUrls);
            return;
        }

        copyIntoTableFromS3(fileUrls);
    }

    private void copyIntoTableFromS3(List<String> s3FileUrls) throws SQLException {
        // Snowflake COPY INTO does not support specifying multiple files directly
        // Import the file directly when there is only one file
        // Import the directory when there are multiple files
        String s3Protocol = config.getS3Protocol();
        String fromLocation;

        if (s3FileUrls.size() == 1) {
            // Single file: import it directly
            String s3Url = convertS3UrlToSnowflakeFormat(s3FileUrls.get(0), s3Protocol);
            fromLocation = "'" + s3Url.replace("'", "''") + "'";
            log.info("Import single file: {}", s3Url);
        } else {
            // Multiple files: import by directory
            // Extract common directory path
            String commonPrefix = findCommonPrefix(s3FileUrls);
            if (commonPrefix != null && !commonPrefix.isEmpty()) {
                // Use directory mode
                String s3Url = convertS3UrlToSnowflakeFormat(commonPrefix, s3Protocol);
                fromLocation = "'" + s3Url.replace("'", "''") + "'";
                log.info(
                        "Import directory: {} (contains {} files)",
                        commonPrefix,
                        s3FileUrls.size());
            } else {
                // Import the first file and log a warning when there is no common prefix
                log.warn(
                        "Multiple files have no common prefix; only the first file will be imported");
                String s3Url = convertS3UrlToSnowflakeFormat(s3FileUrls.get(0), s3Protocol);
                fromLocation = "'" + s3Url.replace("'", "''") + "'";
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("COPY INTO ").append(buildTargetTable()).append(" ");
        sql.append("FROM ").append(fromLocation).append(" ");

        // Add AWS credentials
        String credentialsPart =
                "CREDENTIALS = (AWS_KEY_ID = '"
                        + config.getAwsAccessKeyId().replace("'", "''")
                        + "' AWS_SECRET_KEY = '"
                        + config.getAwsSecretAccessKey().replace("'", "''")
                        + "') ";
        sql.append(credentialsPart);
        log.info(
                "CREDENTIALS section: {}",
                credentialsPart
                        .replace(config.getAwsAccessKeyId(), "***")
                        .replace(config.getAwsSecretAccessKey(), "***"));

        appendInlineFileFormat(sql);
        appendCopyBehavior(sql);

        String copySql = sql.toString();
        log.info("Executing COPY INTO: {}", copySql);
        log.info("S3 file count: {}", s3FileUrls.size());
        log.info("First S3 file: {}", s3FileUrls.isEmpty() ? "none" : s3FileUrls.get(0));
        executeCopySql(copySql, s3FileUrls.size());
    }

    private void copyIntoTableFromLocalFiles(List<String> localFilePaths) throws SQLException {
        List<Path> localPaths =
                localFilePaths.stream().map(Paths::get).collect(Collectors.toList());

        for (Path localPath : localPaths) {
            String putSql = buildPutCommand(localPath, localStageLocation);
            executeSql(putSql);
        }

        List<String> stagedFileNames =
                localPaths.stream()
                        .map(path -> path.getFileName().toString())
                        .collect(Collectors.toList());
        String copySql = buildCopyIntoStageSql(localStageLocation, stagedFileNames);
        log.info("Executing local COPY INTO: {}", copySql);
        executeCopySql(copySql, localFilePaths.size());
    }

    String buildLocalStageLocation() {
        return localStageLocation;
    }

    String buildPutCommand(Path localFile, String stageLocation) {
        String fileUri = localFile.toAbsolutePath().toUri().toString().replace("'", "''");
        return "PUT '" + fileUri + "' " + stageLocation + " AUTO_COMPRESS = FALSE OVERWRITE = TRUE";
    }

    String buildCopyIntoStageSql(String stageLocation, List<String> fileNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("COPY INTO ").append(buildTargetTable()).append(" ");
        sql.append("FROM ").append(ensureTrailingSlash(stageLocation)).append(" ");
        if (fileNames != null && !fileNames.isEmpty()) {
            sql.append("FILES = (")
                    .append(
                            fileNames.stream()
                                    .map(fileName -> "'" + fileName.replace("'", "''") + "'")
                                    .collect(Collectors.joining(",")))
                    .append(") ");
        }
        appendInlineFileFormat(sql);
        appendCopyBehavior(sql);
        return sql.toString();
    }

    private void appendInlineFileFormat(StringBuilder sql) {
        sql.append("FILE_FORMAT = (TYPE = '").append(config.getFileFormat()).append("'");

        if ("CSV".equalsIgnoreCase(config.getFileFormat())) {
            sql.append(" FIELD_DELIMITER = '")
                    .append(config.getFieldDelimiter())
                    .append("'")
                    .append(" RECORD_DELIMITER = '")
                    .append(config.getRecordDelimiter().replace("\n", "\\n"))
                    .append("'")
                    .append(" SKIP_HEADER = 0")
                    .append(" FIELD_OPTIONALLY_ENCLOSED_BY = '\"'")
                    .append(" NULL_IF = ('')");
        }

        sql.append(") ");
    }

    private void appendCopyBehavior(StringBuilder sql) {
        Map<String, String> copyOptions = config.getCopyOptions();
        appendCopyOptionIfAbsent(sql, copyOptions, "ON_ERROR", "'ABORT_STATEMENT'");
        appendCopyOptionIfAbsent(
                sql, copyOptions, "PURGE", config.isPurgeAfterCopy() ? "TRUE" : "FALSE");
        appendCopyOptionIfAbsent(sql, copyOptions, "FORCE", "TRUE");
        for (Map.Entry<String, String> entry : copyOptions.entrySet()) {
            sql.append(entry.getKey()).append(" = ").append(entry.getValue()).append(" ");
        }
    }

    private void appendCopyOptionIfAbsent(
            StringBuilder sql, Map<String, String> copyOptions, String key, String value) {
        if (copyOptions.keySet().stream().noneMatch(option -> option.equalsIgnoreCase(key))) {
            sql.append(key).append(" = ").append(value).append(" ");
        }
    }

    private void executeCopySql(String copySql, int fileCount) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            boolean hasResult = stmt.execute(copySql);
            boolean hasCopyFailure = false;

            if (hasResult) {
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        String file = rs.getString("FILE");
                        String status = rs.getString("STATUS");
                        int rowsLoaded = rs.getInt("ROWS_LOADED");
                        int errorsSeen = rs.getInt("ERRORS_SEEN");

                        log.info(
                                "File: {}, Status: {}, Rows: {}, Errors: {}",
                                file,
                                status,
                                rowsLoaded,
                                errorsSeen);

                        if (errorsSeen > 0 || !"LOADED".equalsIgnoreCase(status)) {
                            log.warn("File {} had {} errors", file, errorsSeen);
                            hasCopyFailure = true;
                        }
                    }
                }
            }

            if (hasCopyFailure) {
                throw new SQLException("COPY INTO completed with failed files, see previous logs");
            }

            log.info("COPY INTO completed successfully for {} files", fileCount);
        }
    }

    private String buildTargetTable() {
        return SnowflakeIdentifierUtils.quoteQualifiedIdentifier(
                config.getDatabase(), config.getSchema(), config.getTable());
    }

    private String initializeLocalStageLocation() {
        if (!config.isLocalFileStagingBackend()) {
            return null;
        }

        String normalizedPrefix = normalizeStagePath(config.getLocalStagePrefix());
        String sessionId = UUID.randomUUID().toString().replace("-", "");
        String stagePath =
                normalizedPrefix.isEmpty() ? sessionId : normalizedPrefix + "/" + sessionId;

        switch (config.getLocalStageType()) {
            case USER:
                return "@~/" + stagePath;
            case TABLE:
                return "@%" + config.getTable() + "/" + stagePath;
            case NAMED:
                String stageName = config.getLocalStageName();
                if (stageName == null || stageName.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "local_stage_name is required when local_stage_type is NAMED");
                }
                String normalizedStageName =
                        stageName.startsWith("@") ? stageName : "@" + stageName;
                return normalizedStageName + "/" + stagePath;
            default:
                throw new IllegalArgumentException(
                        "Unsupported local stage type: " + config.getLocalStageType());
        }
    }

    private String normalizeStagePath(String stagePath) {
        if (stagePath == null) {
            return "";
        }
        String normalized = stagePath.trim();
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private String ensureTrailingSlash(String stageLocation) {
        if (stageLocation == null || stageLocation.isEmpty()) {
            return stageLocation;
        }
        return stageLocation.endsWith("/") ? stageLocation : stageLocation + "/";
    }

    /** Convert an S3 URL to Snowflake format */
    private String convertS3UrlToSnowflakeFormat(String s3Url, String protocol) {
        // Snowflake COPY INTO requires standard S3 URL format
        // Ensure the URL format is correct without extra quote escaping
        if (s3Url == null || s3Url.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 URL cannot be null or empty");
        }

        // Replace protocol prefix
        if (s3Url.startsWith("s3://")) {
            return protocol + "://" + s3Url.substring(5);
        }

        // Return the original URL if it is not in standard s3:// format
        return s3Url;
    }

    /** Find the common prefix of multiple S3 URLs */
    private String findCommonPrefix(List<String> s3Urls) {
        if (s3Urls == null || s3Urls.isEmpty()) {
            return null;
        }

        // Use the shortest URL as the base
        String shortest = s3Urls.get(0);
        for (String url : s3Urls) {
            if (url.length() < shortest.length()) {
                shortest = url;
            }
        }

        // Find common prefix
        String commonPrefix = shortest;
        for (String url : s3Urls) {
            while (!url.startsWith(commonPrefix)) {
                commonPrefix = commonPrefix.substring(0, commonPrefix.length() - 1);
                if (commonPrefix.isEmpty()) {
                    return null;
                }
            }
        }

        // Ensure common prefix ends with a path separator
        if (!commonPrefix.endsWith("/")) {
            int lastSlashIndex = commonPrefix.lastIndexOf('/');
            if (lastSlashIndex > 0) {
                commonPrefix = commonPrefix.substring(0, lastSlashIndex + 1);
            } else {
                return null;
            }
        }

        return commonPrefix;
    }

    /** Get table info */
    public List<String> getTableColumns() throws SQLException {
        List<String> columns = new ArrayList<>();
        String sql = "SHOW COLUMNS IN " + buildTargetTable();

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                columns.add(columnName);
            }
        }

        return columns;
    }

    /** Drop temporary file format */
    public void dropTemporaryFileFormat() throws SQLException {
        if (fileFormatName != null && !fileFormatName.equals(config.getSnowflakeFileFormatName())) {
            String sql = "DROP FILE FORMAT IF EXISTS " + fileFormatName;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
                log.info("Dropped temporary file format: {}", fileFormatName);
            }
        }
    }

    /** Close connection */
    public void close() throws SQLException {
        try {
            dropTemporaryFileFormat();
        } finally {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                log.info("Disconnected from Snowflake");
            }
        }
    }

    /** Get database connection */
    public Connection getConnection() {
        return connection;
    }

    /** Execute SQL statement */
    public void executeSql(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("SQL executed successfully: {}", sql);
        }
    }

    /** Execute query and return whether any row exists */
    public boolean executeQuery(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    /** Execute count query */
    public int executeCountQuery(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
}
