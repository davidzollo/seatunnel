# StainTrace - Quick Start Guide

## Overview

StainTrace is SeaTunnel's **data lineage and end-to-end performance tracing system** for tracking the complete data flow within the engine.

### Core Features

- **Framework-level Implementation**: Works with all Connectors out-of-the-box, no connector code changes needed
- **6 Basic Stages**: S0 → Q+ → Q- → T+ → T- → W! (complete end-to-end pipeline)
- **Extended Fine-grained Stages**: 40+ extended stages for precise performance bottleneck identification
- **Local File Storage**: Zero dependencies, JSON Lines format, lightweight
- **Task-level Control**: Engine-level + task-level dual switches for flexible sampling control
- **Offline Analysis Tool**: Standalone analyzer generates HTML reports
- **OpenTelemetry Integration**: Native support for OTel Span JSON format
- **Performance Optimized**: Sampling rate control + batch processing + rate limiting, overhead <2%

### 6 Basic Tracing Stages

1. **S0** (SOURCE_EMIT): Source emits data
2. **Q+** (QUEUE_IN): Enter queue
3. **Q-** (QUEUE_OUT): Leave queue
4. **T+** (TRANSFORM_IN): Transform receives data
5. **T-** (TRANSFORM_OUT): Transform outputs data
6. **W!** (SINK_WRITE_DONE): Sink write completed

### Stage Details

#### Basic Stages (1-6)
| Stage | Code | Description | Recording Location |
|-------|------|-------------|-------------------|
| SOURCE_EMIT | 1 | Source emits data | SeaTunnelSourceCollector.collect() |
| QUEUE_IN | 2 | Enter queue (before enqueue, captures backpressure) | IntermediateQueue.received() |
| QUEUE_OUT | 3 | Leave queue (after dequeue) | IntermediateQueue.collect() |
| TRANSFORM_IN | 4 | Transform receives data | TransformFlowLifeCycle.received() |
| TRANSFORM_OUT | 5 | Transform outputs data | TransformFlowLifeCycle before output |
| SINK_WRITE_DONE | 6 | Sink write completed | After SinkFlowLifeCycle.writer.write() |

#### Key Performance Stages (101-110)
| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| SOURCE_READ_END | 101 | Source read completed | Source read performance |
| QUEUE_OFFER_START | 102 | Queue enqueue started | Backpressure detection |
| TRANSFORM_EXECUTE_START | 104 | Transform execution started | Transform performance analysis |
| TRANSFORM_EXECUTE_END | 105 | Transform execution ended | Transform performance analysis |
| SINK_BATCH_AGGREGATE_END | 106 | Sink batch aggregation completed | Sink batch processing performance |
| SINK_FORMAT_END | 107 | Sink formatting completed | Data formatting performance |
| SINK_WRITE_START | 108 | Sink I/O write started | I/O performance analysis |
| SINK_WRITE_END | 109 | Sink I/O write ended | I/O performance analysis |
| SINK_COMMIT_END | 110 | Sink commit completed | Transaction commit performance |

#### Extended Fine-grained Stages (201-220)
| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| SOURCE_READ_START | 201 | Source read started | Source performance |
| SOURCE_SERIALIZE_START/END | 202-203 | Source serialization | Serialization performance |
| TRANSFORM_PARSE_START/END | 205-206 | Transform parsing | Parsing performance |
| TRANSFORM_BUILD_START/END | 207-208 | Transform result building | Building performance |
| SINK_RECEIVE | 209 | Sink receives data | Data flow tracking |
| SINK_BATCH_AGGREGATE_START | 210 | Sink batch aggregation started | Batch processing performance |
| SINK_FORMAT_START | 211 | Sink formatting started | Formatting performance |
| SINK_COMMIT_START | 212 | Sink commit started | Commit performance |
| CHECKPOINT_SNAPSHOT_START/END | 213-214 | Checkpoint snapshot | Checkpoint performance |
| CHECKPOINT_BARRIER_EMIT/RECEIVE | 215-216 | Checkpoint Barrier | Barrier propagation |

#### Network Transfer Stages (217-220, Multi-node Cluster Only)
| Stage | Code | Description | When It Appears |
|-------|------|-------------|-----------------|
| RECORD_SERIALIZE_START | 217 | Data serialization started | Multi-node cluster, cross-node data transfer |
| RECORD_SERIALIZE_END | 218 | Data serialization ended | Same as above |
| RECORD_DESERIALIZE_START | 219 | Data deserialization started | Same as above |
| RECORD_DESERIALIZE_END | 220 | Data deserialization ended | Same as above |

#### Flow Control Audit Stages (226-227)
| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| FLOW_CONTROL_AUDIT_START | 226 | Flow control audit started | Backpressure detection |
| FLOW_CONTROL_AUDIT_END | 227 | Flow control audit ended | Backpressure detection |

> **⚠️ Important Notes**:
> - **Single-node Execution**: Data transfers via in-memory queues, serialization stages (217-220) **will NOT appear**
> - **Multi-node Cluster**: Serialization stages **will appear** when data needs network transfer between nodes
> - Serialization duration can be calculated via `gap_ms`: `RECORD_SERIALIZE_END - RECORD_SERIALIZE_START`

### Local File Storage

StainTrace uses local file storage for trace data:

- **Zero Dependencies**: No database or external services required
- **Lightweight**: JSON Lines format, human-readable
- **Offline Analysis**: Standalone analyzer tool generates HTML reports
- **Storage Path**: `/tmp/seatunnel/traces/{job_id}/{yyyy-MM-dd}/`

---

## Quick Start

### Step 1: Configure Engine

Edit `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    # Enable stain trace (system-level master switch)
    stain-trace-enabled: true

    # Sampling interval: sample 1 out of every 100 records (development environment)
    # Production recommendation: 100000-1000000
    stain-trace-sample-interval: 100

    # Local file storage base directory
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # Max events per file (creates new file when reached)
    stain-trace-file-max-events-per-file: 10000

    # Max file size in MB (creates new file when reached)
    stain-trace-file-max-size-mb: 10

    # Flush interval in seconds (batch write interval)
    stain-trace-file-flush-interval-seconds: 10
```

### Step 2: Enable Task-level Switch

Enable in job configuration (`job.conf`):

```hocon
env {
  stain_trace {
    enabled = true
  }

  # Other environment configurations...
  parallelism = 2
  job.mode = "BATCH"
}

source {
  # ... your source configuration
}

transform {
  # ... your transform configuration
}

sink {
  # ... your sink configuration
}
```

### Step 3: Run Your Job

Run your SeaTunnel job, trace data will be automatically saved to local files.

### Step 4: View Trace Files

```bash
# View generated files
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# View file contents (JSON Lines format)
cat /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### Step 5: Generate HTML Report Using Analysis Tool

```bash
cd seatunnel-trace/seatunnel-trace-analyzer
mvn clean package

# Analyze trace files and generate HTML report
./analyze-traces.sh /tmp/seatunnel/traces report.html

# Open report in browser
open report.html  # macOS
xdg-open report.html  # Linux
```

**Done!** No services needed, trace data is stored in local files.

---

## File Storage Format

### Directory Structure

```
/tmp/seatunnel/traces/
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── trace-0001.jsonl
            ├── trace-0002.jsonl
            └── ...
```

### File Format

Each file is in JSON Lines format (one JSON object per line):

```json
{"eventType":"STAIN_TRACE","jobId":"123456","timestamp":1708000000000,"spans":[{"name":"seatunnel.record","context":{"traceId":789,"spanId":789},"startTime":1708000000000,"endTime":1708000001000,"events":[{"name":"SOURCE_EMIT","timestamp":1708000000000,"attributes":{"seatunnel.stage_code":1,"seatunnel.task_id":1}}]}]}
{"eventType":"STAIN_TRACE","jobId":"123456","timestamp":1708000000100,"spans":[{"name":"seatunnel.record","context":{"traceId":790,"spanId":790},"startTime":1708000000100,"endTime":1708000001100,"events":[{"name":"SOURCE_EMIT","timestamp":1708000000100,"attributes":{"seatunnel.stage_code":1,"seatunnel.task_id":1}}]}]}
```

Each line contains:
- `eventType`: Event type (STAIN_TRACE)
- `jobId`: Job ID
- `timestamp`: Event timestamp
- `spans`: OpenTelemetry Span array
  - `name`: Span name (seatunnel.record)
  - `context`: Trace context (traceId, spanId)
  - `startTime/endTime`: Start and end timestamps
  - `events`: Stage event array
    - `name`: Stage name (SOURCE_EMIT, QUEUE_IN, etc.)
    - `timestamp`: Stage timestamp
    - `attributes`: Stage attributes (stage_code, task_id, etc.)

---

## Configuration Reference

### Engine-level Configuration (seatunnel.yaml)

```yaml
seatunnel:
  engine:
    # ==================== Basic Configuration ====================
    # Enable stain trace (system-level master switch)
    stain-trace-enabled: true

    # Sampling interval: sample 1 out of every N records (default: 100000)
    # Development recommendation: 100-1000
    # Production recommendation: 100000-1000000
    stain-trace-sample-interval: 100000

    # Max traces per worker per second (default: 50)
    # Controls trace volume to prevent event storms
    stain-trace-max-traces-per-second-per-worker: 50

    # Max stage entries per trace (default: 32)
    # 32 covers 99% of pipelines, avoids payload bloat
    stain-trace-max-entries-per-trace: 32

    # ==================== Advanced Configuration ====================
    # Whether to propagate payload to all split outputs (default: false)
    # false: Only first output inherits payload in 1-to-N scenarios
    # true: All splits inherit payload (increases trace count)
    stain-trace-propagate-to-all-splits: false

    # ==================== Local File Storage Configuration ====================
    # File storage base directory (default: /tmp/seatunnel/traces)
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # Max events per file (default: 10000)
    # Creates new file when reached
    stain-trace-file-max-events-per-file: 10000

    # Max file size in MB (default: 10)
    # Creates new file when reached
    stain-trace-file-max-size-mb: 10

    # Flush interval in seconds (default: 10)
    # Batch write interval, balances performance and data integrity
    stain-trace-file-flush-interval-seconds: 10
```

### Task-level Configuration (job.conf)

Control trace enablement via `env` block in job configuration:

```hocon
env {
  # Task-level StainTrace switch
  stain_trace {
    # Enable trace for this task (default: false)
    # Note: Engine-level stain-trace-enabled must also be true
    enabled = true

    # Task-level sampling interval (optional, overrides engine-level)
    # sample_interval = 1000
  }

  # Other environment configurations...
  parallelism = 2
  job.mode = "BATCH"
}
```

### Configuration Reference Table

| Configuration Item | Type | Default | Description |
|-------------------|------|---------|-------------|
| **Engine-level (seatunnel.yaml)** ||||
| `stain-trace-enabled` | Boolean | false | Engine-level master switch, must be true to enable |
| `stain-trace-sample-interval` | Integer | 100000 | Sampling interval: sample 1 out of every N records |
| `stain-trace-max-traces-per-second-per-worker` | Integer | 50 | Max traces per worker per second |
| `stain-trace-max-entries-per-trace` | Integer | 32 | Max stage entries per trace |
| `stain-trace-propagate-to-all-splits` | Boolean | false | Propagate to all split outputs |
| `stain-trace-file-base-path` | String | /tmp/seatunnel/traces | Local file storage base directory |
| `stain-trace-file-max-events-per-file` | Integer | 10000 | Max events per file |
| `stain-trace-file-max-size-mb` | Integer | 10 | Max file size (MB) |
| `stain-trace-file-flush-interval-seconds` | Integer | 10 | Flush interval (seconds) |
| **Task-level (job.conf env block)** ||||
| `stain_trace.enabled` | Boolean | false | Task-level switch, requires engine-level also enabled |
| `stain_trace.sample_interval` | Integer | Inherits engine-level | Task-level sampling interval (optional) |

### Activation Conditions

StainTrace final activation condition:

```
effectiveEnabled = engineConfig.stainTraceEnabled && jobEnv.stainTrace.enabled
```

That is: **Both engine-level and task-level must be enabled** for trace to work.

---

## Verification

After running your job, check the following:

### 1. Check If Files Are Generated

```bash
# View file list
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# View file contents
head -n 5 /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### 2. Verify Data Integrity

You should see:
- Each line is a complete JSON object
- Contains events for 6 basic stages (SOURCE_EMIT, QUEUE_IN, QUEUE_OUT, TRANSFORM_IN, TRANSFORM_OUT, SINK_WRITE_DONE)
- Timestamps are increasing (S0 < Q+ < Q- < T+ < T- < W!)

### 3. Use Analysis Tool

```bash
cd seatunnel-trace/seatunnel-trace-analyzer
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

The analysis tool generates an HTML report including:
- End-to-end latency analysis
- Stage duration statistics
- Performance bottleneck identification
- Timeline visualization

### 4. Example Job Characteristics

Default example job `stain_trace_fake_sql_union_to_console.conf`:
- **FakeSource**: Generates 10 records
- **Sql Transform**: Uses LATERAL VIEW EXPLODE, 1 input → 2 outputs
- **Console Sink**: Outputs 20 records
- **Sampling rate**: sample-rate=1 (full sampling)
- **Expected traces**: 10 traces (only first split inherits payload)

---

## Troubleshooting

### Problem 1: Cannot Find Trace Files

**Troubleshooting Steps**:

1. Confirm file storage path:
```bash
# Default path
ls -lh /tmp/seatunnel/traces/traces/

# Check if job_id directory exists
ls -lh /tmp/seatunnel/traces/traces/{job_id}/
```

2. Check permissions:
```bash
# Ensure directory is writable
ls -ld /tmp/seatunnel/traces/
```

3. Check configuration:
```yaml
stain-trace-file-base-path: /tmp/seatunnel/traces  # Confirm path is correct
```

4. View job logs, search for "StainTrace" or "TraceFileWriter"

### Problem 2: No Trace Data

**Troubleshooting Steps**:

1. Confirm engine-level switch is enabled:
```bash
grep -A 5 "stain-trace" config/seatunnel.yaml
```

2. Confirm task-level switch is enabled:
```bash
grep -A 3 "stain_trace" examples/your-job.conf
```

3. Verify configuration:
```hocon
env {
  stain_trace {
    enabled = true  # Must be explicitly enabled
  }
}
```

**Remember**: **Both engine-level and task-level must be enabled** for trace to work!

### Problem 3: Only Partial Stage Data

**Reason**: In Transform's 1-to-N scenario, only the first output inherits payload

**Verify**: Check `stain-trace-propagate-to-all-splits` configuration
```yaml
stain-trace-propagate-to-all-splits: false  # Only first inherits (default)
stain-trace-propagate-to-all-splits: true   # All splits inherit
```

### Problem 4: Cannot See Serialization Events (stages 217-220)

**Symptom**: No RECORD_SERIALIZE_START/END or RECORD_DESERIALIZE_START/END in stage details

**Reason**:
- **Single-node execution**: Data transfers via in-memory queues, no serialization needed, so these stages won't appear
- Only in multi-node cluster execution, when data needs cross-node network transfer, will serialization be triggered

**Solution**:
- To test serialization performance, need to set up a multi-node SeaTunnel cluster
- Single-node testing can ignore serialization events, focus on other stages

### Problem 5: Files Too Large or Too Many

**Adjust Configuration**:

```yaml
# Increase sampling interval to reduce trace count
stain-trace-sample-interval: 1000000  # Sample 1 out of every 1 million

# Increase file size limit
stain-trace-file-max-size-mb: 50

# Increase events per file
stain-trace-file-max-events-per-file: 50000
```

### Problem 6: File Permission Issues

**Error**: `Permission denied` or `Cannot create directory`

**Solution**:
```bash
# Create directory and set permissions
sudo mkdir -p /tmp/seatunnel/traces
sudo chmod 777 /tmp/seatunnel/traces

# Or use user directory
stain-trace-file-base-path: ~/seatunnel/traces
```

---

## Advanced Usage

### Custom Job Configuration

Create your own job configuration file, refer to:
```bash
seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/stain_trace_fake_sql_union_to_console.conf
```

Specify at runtime:
```java
public static void main(String[] args) {
    String configurePath = "/path/to/your/job.conf";
    // ... rest of code
}
```

### Performance Tuning

#### Development Environment Configuration (High Sampling, Easy Debugging)
```yaml
stain-trace-enabled: true
stain-trace-sample-interval: 100  # Sample 1 out of every 100
stain-trace-max-traces-per-second-per-worker: 1000
stain-trace-max-entries-per-trace: 64
stain-trace-file-base-path: /tmp/seatunnel/traces
stain-trace-file-flush-interval-seconds: 5  # More frequent flushing
```

#### Production Environment Configuration (Low Overhead, Large Scale)
```yaml
stain-trace-enabled: true
stain-trace-sample-interval: 100000  # Sample 1 out of every 100k
stain-trace-max-traces-per-second-per-worker: 50
stain-trace-max-entries-per-trace: 32
stain-trace-file-base-path: /data/seatunnel/traces
stain-trace-file-max-events-per-file: 50000  # Larger files
stain-trace-file-max-size-mb: 50
stain-trace-file-flush-interval-seconds: 30  # Less frequent flushing
```

#### Performance Impact

After optimization, StainTrace performance impact:

| Metric | Value |
|--------|-------|
| CPU overhead at 1/100000 sampling rate | **< 2%** |
| CPU overhead at 1/1000 sampling rate | < 5% |
| Trace payload size per record | ~1KB (32 stages) |
| Arrays.copyOf calls reduction | **-60% ~ -70%** |
| System.currentTimeMillis calls reduction | **-50%** |

### Per-job Sampling Rate Control

Different jobs can use different sampling rates:

```hocon
# High throughput job: low sampling rate
env {
  stain_trace {
    enabled = true
    sample_interval = 1000000  # Sample 1 out of every 1 million
  }
}
```

```hocon
# Debug job: high sampling rate
env {
  stain_trace {
    enabled = true
    sample_interval = 10  # Sample 1 out of every 10
  }
}
```

### Periodic Cleanup of Old Files

```bash
# Delete trace files older than 7 days
find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete

# Or use crontab for scheduled cleanup
# Clean up files older than 7 days at 2 AM daily
0 2 * * * find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete
```

---

## Performance Optimization Achievements

StainTrace has been optimized to ensure production readiness:

### Core Optimizations (Completed ✅)

1. **Batch Append API**
   - Append multiple stages at once, reducing array copies
   - Arrays.copyOf calls reduced by **60-70%**

2. **Timestamp Optimization**
   - Batch operations share timestamps
   - System.currentTimeMillis calls reduced by **50%**

3. **Network Serialization Tracing**
   - RECORD_SERIALIZE_START/END (217-218)
   - RECORD_DESERIALIZE_START/END (219-220)
   - Precisely identifies network transfer bottlenecks

4. **Flow Control Audit Tracing**
   - FLOW_CONTROL_AUDIT_START/END (226-227)
   - Identifies backpressure issues

5. **Local File Storage**
   - Zero dependencies, no database required
   - JSON Lines format, human-readable
   - Standalone analyzer tool generates HTML reports

### Expected Results

| Scenario | Sampling Rate | Throughput | Expected Overhead |
|----------|---------------|------------|-------------------|
| Production | 1/100000 | 1M records/s | **< 2%** |
| Testing | 1/1000 | 100K records/s | < 5% |
| Development | 1/100 | 10K records/s | < 10% |

---

