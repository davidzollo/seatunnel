# SnowflakeFile Connector

SnowflakeFile Connector for SeaTunnel enables high-performance data loading into Snowflake using staged files.

## Overview

This connector supports two staging backends:

1. **S3**: Write files to Amazon S3, then run `COPY INTO` from the external location.
2. **LOCAL_FILE**: Write files to a local temp directory, upload them with Snowflake `PUT`, then run `COPY INTO` from an internal stage.

The workflow is:

1. **Buffer Data**: Accumulate data in memory buffers
2. **Stage Files**: Write data to S3 or local temp files
3. **Copy into Snowflake**: Use Snowflake's `COPY INTO` command to load staged files
4. **Cleanup**: Optionally purge staged files after successful load

## Features

- ✅ **High Performance**: Memory buffering + S3 multipart uploads
- ✅ **Multiple Backends**: S3 and local file staging
- ✅ **Partitioned Writing**: Automatic data partitioning for parallel processing
- ✅ **Configurable File Sizes**: Automatic file rolling based on size
- ✅ **Error Handling**: Continue on errors with detailed logging
- ✅ **Automatic Cleanup**: Optional S3 file purge after successful copy

Current writer implementation is production-ready for CSV serialization. Other format names may exist in configuration, but local-file loading should currently be treated as CSV-focused unless matching serializers are added.

## Configuration

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| account | String | Snowflake account identifier |
| warehouse | String | Snowflake warehouse name |
| database | String | Target database name |
| schema | String | Target schema name |
| table | String | Target table name |
| user | String | Snowflake username |
| password | String | Snowflake password |
| staging_backend | Enum | Staging backend: `S3` or `LOCAL_FILE` |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| s3_bucket | String | null | S3 bucket for staging files when `staging_backend = S3` |
| aws_access_key_id | String | null | AWS Access Key ID when `staging_backend = S3` |
| aws_secret_access_key | String | null | AWS Secret Access Key when `staging_backend = S3` |
| role | String | null | Snowflake role |
| s3_region | String | us-east-1 | AWS S3 region |
| s3_key_prefix | String | snowflake-staging/ | S3 key prefix for staging files |
| local_temp_dir | String | `${java.io.tmpdir}/seatunnel-snowflake-file` | Local temp directory used before `PUT` |
| local_stage_type | Enum | USER | Internal stage type for `LOCAL_FILE`: `USER`, `TABLE`, `NAMED` |
| local_stage_name | String | null | Named internal stage when `local_stage_type = NAMED` |
| local_stage_prefix | String | seatunnel-local | Path prefix used in the internal stage |
| file_format | Enum | CSV | File format: CSV, JSON, PARQUET, AVRO, ORC |
| field_delimiter | String | , | Field delimiter for CSV |
| record_delimiter | String | \n | Record delimiter |
| file_extension | String | .csv | File extension |
| buffer_size | Integer | 1048576 | Buffer size in bytes (1MB) |
| max_file_size | Long | 104857600 | Max file size before rolling (100MB) |
| purge_after_copy | Boolean | true | Purge S3 files after successful copy |
| time_format | String | HH24:MI:SS | Time format for Snowflake |
| date_format | String | YYYY-MM-DD | Date format for Snowflake |
| timestamp_format | String | YYYY-MM-DD HH24:MI:SS.FF3 | Timestamp format |
| snowflake_file_format_name | String | null | Use existing file format |
| copy_options | Map | {} | Additional COPY INTO options |

## Examples

### Basic CSV Example

```hocon
sink {
  SnowflakeFile {
    account = "myaccount"
    warehouse = "COMPUTE_WH"
    database = "MY_DB"
    schema = "PUBLIC"
    table = "users"
    user = "myuser"
    password = "mypassword"

    s3_bucket = "my-snowflake-staging"
    s3_region = "us-west-2"
    aws_access_key_id = "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
  }
}
```

### Parquet Format Example

```hocon
sink {
  SnowflakeFile {
    account = "myaccount"
    warehouse = "COMPUTE_WH"
    database = "ANALYTICS"
    schema = "DATA"
    table = "events"
    user = "myuser"
    password = "mypassword"

    s3_bucket = "analytics-data"
    s3_region = "us-east-1"
    s3_key_prefix = "snowflake/events/"
    aws_access_key_id = "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY}"

    file_format = "PARQUET"
    max_file_size = 268435456  # 256MB
    buffer_size = 4194304      # 4MB
  }
}
```

### Local File Example

```hocon
sink {
  SnowflakeFile {
    account = "myaccount"
    warehouse = "COMPUTE_WH"
    database = "MY_DB"
    schema = "PUBLIC"
    table = "users"
    user = "myuser"
    password = "mypassword"

    staging_backend = "LOCAL_FILE"
    local_stage_type = "USER"
    local_stage_prefix = "seatunnel-local"
    local_temp_dir = "/tmp/seatunnel-snowflake-file"

    file_format = "CSV"
    field_delimiter = ","
    record_delimiter = "\n"
  }
}
```

### Advanced Configuration

```hocon
sink {
  SnowflakeFile {
    # Snowflake connection
    account = "myaccount"
    warehouse = "COMPUTE_WH"
    database = "PRODUCTION"
    schema = "PUBLIC"
    table = "transactions"
    user = "etl_user"
    password = "${PASSWORD}"  # Use environment variable
    role = "ETL_ROLE"

    # S3 configuration
    s3_bucket = "prod-snowflake-staging"
    s3_region = "eu-west-1"
    s3_key_prefix = "transactions/2024/01/"
    aws_access_key_id = "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY}"

    # File format
    file_format = "CSV"
    field_delimiter = "|"
    record_delimiter = "\n"
    file_extension = ".csv.gz"  # Gzipped CSV

    # Performance tuning
    buffer_size = 2097152      # 2MB buffer
    max_file_size = 52428800   # 50MB files
    purge_after_copy = true

    # Date/time formats
    time_format = "HH24:MI:SS"
    date_format = "YYYY-MM-DD"
    timestamp_format = "YYYY-MM-DD HH24:MI:SS.FF9"

    # Copy options
    copy_options = {
      "ON_ERROR" = "SKIP_FILE_3"      # Skip file after 3 errors
      "FORCE" = "TRUE"                 # Force reload
      "TRIM_SPACE" = "TRUE"            # Trim whitespace
      "NULL_IF" = "('NULL', '')"       # Treat NULL and empty as null
      "ERROR_ON_COLUMN_COUNT_MISMATCH" = "FALSE"
    }
  }
}
```

## Performance Tuning

### File Size Optimization
- **Small files** (< 10MB): Higher overhead, more COPY commands
- **Large files** (> 500MB): Better compression, fewer COPY commands, but longer processing
- **Recommended**: 50-200MB per file

### Buffer Configuration
- **buffer_size**: Larger buffers reduce S3 API calls but use more memory
- **max_file_size**: Balance between parallelism and efficiency

### Parallelism
- Connector automatically partitions data based on row hash
- Each partition writes independently to S3
- Snowflake can load multiple files in parallel

### S3 Optimization
- Use appropriate S3 region to minimize latency
- Consider S3 Transfer Acceleration for large datasets
- Monitor S3 request costs with large numbers of small files

## Error Handling

The connector provides comprehensive error handling:

1. **S3 Upload Errors**: Retries with exponential backoff
2. **Snowflake COPY Errors**: Detailed error logging, option to continue on errors
3. **File Format Errors**: Configurable error handling via `copy_options`
4. **Network Issues**: Automatic reconnection for transient failures

## Monitoring

Key metrics logged:
- Number of files uploaded to S3
- Total rows and data volume
- COPY INTO execution statistics
- Error counts and details
- File cleanup status

## Limitations

- `LOCAL_FILE` relies on Snowflake JDBC `PUT`, so the runtime must allow `PUT/GET`
- `LOCAL_FILE` requires writable local disk space until upload finishes
- `LOCAL_FILE` currently targets internal stages and is best treated as CSV-first
- `S3` requires AWS S3 bucket access
- Snowflake account must have appropriate permissions
- Maximum file size limited by S3 (5TB) and memory
- Single AWS region per connector instance

## Security

- AWS credentials should be properly secured
- Use IAM roles when possible instead of access keys
- Enable S3 bucket encryption
- Configure appropriate S3 bucket policies
- Use Snowflake role-based access control

## Snowflake 写入

```sql
COPY INTO MY_TABLE
  FROM 's3china://wt-auto-bucket/snow/'
  CREDENTIALS = (
      AWS_KEY_ID = '<AWS_ACCESS_KEY_ID>'
      AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  )
  FILE_FORMAT = (
      TYPE = 'CSV'
      FIELD_DELIMITER = ','
      SKIP_HEADER = 1
      NULL_IF = ('')
      EMPTY_FIELD_AS_NULL = TRUE
      ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  PATTERN = '.*[.]csv'
  ON_ERROR = 'CONTINUE';
```
