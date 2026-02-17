# StainTrace

## Overview

StainTrace is SeaTunnel's data lineage and end-to-end performance tracing system for tracking complete data flow within the Zeta engine.

## Core Features

- **Framework-level Implementation**: Works with all Connectors automatically
- **6 Basic Stages**: SOURCE_EMIT → QUEUE_IN → QUEUE_OUT → TRANSFORM_IN → TRANSFORM_OUT → SINK_WRITE_DONE
- **Extended Stages**: 40+ fine-grained stages for precise bottleneck identification
- **Local File Storage**: Zero dependencies, JSON Lines format
- **Performance Optimized**: < 2% overhead with proper sampling configuration

## Tracing Stages

### Basic Stages (1-6)

| Stage Code | Name | Description |
|-----------|------|-------------|
| 1 | SOURCE_EMIT (S0) | Source emits data |
| 2 | QUEUE_IN (Q+) | Data enters queue |
| 3 | QUEUE_OUT (Q-) | Data leaves queue |
| 4 | TRANSFORM_IN (T+) | Transform receives data |
| 5 | TRANSFORM_OUT (T-) | Transform outputs data |
| 6 | SINK_WRITE_DONE (W!) | Sink write completed |

### Performance Stages (101-110)

For detailed performance analysis:
- SOURCE_READ_END (101)
- QUEUE_OFFER_START (102)
- TRANSFORM_EXECUTE_START/END (104-105)
- SINK_BATCH_AGGREGATE_END (106)
- SINK_WRITE_START/END (108-109)
- SINK_COMMIT_END (110)

### Fine-grained Stages (201-220)

- Source: READ_START (201), SERIALIZE (202-203)
- Transform: PARSE (205-206), BUILD (207-208)
- Sink: RECEIVE (209), BATCH_AGGREGATE (210), FORMAT (211), COMMIT (212)
- Checkpoint: SNAPSHOT (213-214), BARRIER (215-216)
- Network (multi-node only): SERIALIZE (217-218), DESERIALIZE (219-220)

### Flow Control Stages (226-227)

- FLOW_CONTROL_AUDIT_START/END (226-227): Backpressure detection

## Quick Start

### 1. Configure Engine

Edit `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # Sample 1 out of 100k records
    stain-trace-file-base-path: /tmp/seatunnel/traces
```

### 2. Enable in Job

Edit your job configuration file:

```hocon
env {
  stain_trace {
    enabled = true
  }
}
```

**Important**: Both engine-level and task-level switches must be enabled.

### 3. Run Job

Execute your SeaTunnel job. Trace files will be generated automatically.

### 4. View Traces

```bash
# Check generated files
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{date}/

# View trace data (JSON Lines format)
cat /tmp/seatunnel/traces/traces/{job_id}/{date}/trace-*.jsonl | jq .
```

### 5. Generate Analysis Report

```bash
cd seatunnel-trace/seatunnel-trace-analyzer
mvn clean package
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

## Configuration Reference

### Engine Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| stain-trace-enabled | boolean | false | Master switch to enable tracing |
| stain-trace-sample-interval | int | 100000 | Sample 1 out of every N records |
| stain-trace-max-traces-per-second-per-worker | int | 50 | Max traces per worker per second |
| stain-trace-max-entries-per-trace | int | 32 | Max stage entries per trace |
| stain-trace-propagate-to-all-splits | boolean | false | Propagate to all split outputs |
| stain-trace-file-base-path | string | /tmp/seatunnel/traces | File storage directory |
| stain-trace-file-max-events-per-file | int | 10000 | Max events per file |
| stain-trace-file-max-size-mb | int | 10 | Max file size in MB |
| stain-trace-file-flush-interval-seconds | int | 10 | Flush interval in seconds |

### Task Configuration

```hocon
env {
  stain_trace {
    enabled = true                # Task-level switch
    sample_interval = 1000        # Optional: override engine-level
  }
}
```

## File Format

### Directory Structure

```
/tmp/seatunnel/traces/
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── trace-0001.jsonl
            └── trace-0002.jsonl
```

### JSON Lines Format

Each line is a complete JSON event:

```json
{
  "eventType": "STAIN_TRACE",
  "jobId": "123456",
  "timestamp": 1708000000000,
  "spans": [{
    "name": "seatunnel.record",
    "context": {"traceId": 789, "spanId": 789},
    "startTime": 1708000000000,
    "endTime": 1708000001000,
    "events": [
      {"name": "SOURCE_EMIT", "timestamp": 1708000000000, "attributes": {"seatunnel.stage_code": 1}},
      {"name": "QUEUE_IN", "timestamp": 1708000000100, "attributes": {"seatunnel.stage_code": 2}}
    ]
  }]
}
```

## Performance Impact

| Scenario | Sampling Rate | Throughput | Expected Overhead |
|----------|---------------|------------|-------------------|
| Production | 1/100000 | 1M records/s | < 2% |
| Testing | 1/1000 | 100K records/s | < 5% |
| Development | 1/100 | 10K records/s | < 10% |

## Analysis Tool

The standalone analyzer tool generates HTML reports with:

- End-to-end latency analysis
- Stage duration statistics
- Performance bottleneck identification
- Timeline visualization

## Troubleshooting

### No trace files generated

1. Check both switches are enabled (engine + task level)
2. Verify directory permissions for `stain-trace-file-base-path`
3. Check logs for "StainTrace" or "TraceFileWriter" messages

### Empty trace files

Empty files are normal - they are pre-created for file rotation but may not be used if the job ends early. Safe to delete.

### Only partial stage data

This occurs when Transform splits 1 record into N records. By default, only the first output inherits the trace payload. Set `stain-trace-propagate-to-all-splits: true` to trace all splits.

## See Also

- [Quick Start Guide](../../../seatunnel-trace/STAIN_TRACE_QUICKSTART.md)
- [Event Listener](../event-listener.md)
- [Telemetry](telemetry.md)
