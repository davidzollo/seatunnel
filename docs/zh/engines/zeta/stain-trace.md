# 染色追踪（StainTrace）

## 概述

StainTrace 是 SeaTunnel 的数据血缘与端到端性能追踪系统，用于追踪 Zeta 引擎中数据的完整流转过程。

## 核心特性

- **框架级实现**：所有 Connector 自动支持，无需修改连接器代码
- **6 个基础阶段**：SOURCE_EMIT → QUEUE_IN → QUEUE_OUT → TRANSFORM_IN → TRANSFORM_OUT → SINK_WRITE_DONE
- **扩展阶段**：40+ 个细粒度阶段，精确定位性能瓶颈
- **本地文件存储**：零依赖，JSON Lines 格式
- **性能优化**：合理采样配置下开销 < 2%

## 追踪阶段

### 基础阶段（1-6）

| 阶段代码 | 名称 | 说明 |
|---------|------|------|
| 1 | SOURCE_EMIT (S0) | Source 发出数据 |
| 2 | QUEUE_IN (Q+) | 数据进入队列 |
| 3 | QUEUE_OUT (Q-) | 数据离开队列 |
| 4 | TRANSFORM_IN (T+) | Transform 接收数据 |
| 5 | TRANSFORM_OUT (T-) | Transform 输出数据 |
| 6 | SINK_WRITE_DONE (W!) | Sink 写入完成 |

### 性能阶段（101-110）

用于详细性能分析：
- SOURCE_READ_END (101)
- QUEUE_OFFER_START (102)
- TRANSFORM_EXECUTE_START/END (104-105)
- SINK_BATCH_AGGREGATE_END (106)
- SINK_WRITE_START/END (108-109)
- SINK_COMMIT_END (110)

### 细粒度阶段（201-220）

- Source：READ_START (201)、SERIALIZE (202-203)
- Transform：PARSE (205-206)、BUILD (207-208)
- Sink：RECEIVE (209)、BATCH_AGGREGATE (210)、FORMAT (211)、COMMIT (212)
- Checkpoint：SNAPSHOT (213-214)、BARRIER (215-216)
- 网络传输（仅多节点）：SERIALIZE (217-218)、DESERIALIZE (219-220)

### 流控阶段（226-227）

- FLOW_CONTROL_AUDIT_START/END (226-227)：反压检测

## 快速开始

### 1. 配置引擎

编辑 `seatunnel.yaml`：

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # 每 10 万条采样 1 条
    stain-trace-file-base-path: /tmp/seatunnel/traces
```

### 2. 启用任务级开关

编辑作业配置文件：

```hocon
env {
  stain_trace {
    enabled = true
  }
}
```

**重要**：引擎级和任务级开关都必须启用。

### 3. 运行作业

执行 SeaTunnel 作业，追踪文件会自动生成。

### 4. 查看追踪数据

```bash
# 查看生成的文件
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{date}/

# 查看追踪数据（JSON Lines 格式）
cat /tmp/seatunnel/traces/traces/{job_id}/{date}/trace-*.jsonl | jq .
```

### 5. 生成分析报告

```bash
cd seatunnel-trace/seatunnel-trace-analyzer
mvn clean package
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

## 配置参考

### 引擎级配置

| 参数 | 类型 | 默认值 | 说明 |
|-----|------|--------|------|
| stain-trace-enabled | boolean | false | 启用追踪的主开关 |
| stain-trace-sample-interval | int | 100000 | 每 N 条记录采样 1 条 |
| stain-trace-max-traces-per-second-per-worker | int | 50 | 每个 Worker 每秒最大追踪数 |
| stain-trace-max-entries-per-trace | int | 32 | 每条追踪最大阶段条目数 |
| stain-trace-propagate-to-all-splits | boolean | false | 是否传播到所有分裂输出 |
| stain-trace-file-base-path | string | /tmp/seatunnel/traces | 文件存储目录 |
| stain-trace-file-max-events-per-file | int | 10000 | 每个文件最大事件数 |
| stain-trace-file-max-size-mb | int | 10 | 文件最大大小（MB） |
| stain-trace-file-flush-interval-seconds | int | 10 | 刷盘间隔（秒） |

### 任务级配置

```hocon
env {
  stain_trace {
    enabled = true                # 任务级开关
    sample_interval = 1000        # 可选：覆盖引擎级配置
  }
}
```

## 文件格式

### 目录结构

```
/tmp/seatunnel/traces/
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── trace-0001.jsonl
            └── trace-0002.jsonl
```

### JSON Lines 格式

每行是一个完整的 JSON 事件：

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

## 性能影响

| 场景 | 采样率 | 吞吐量 | 预期开销 |
|------|--------|--------|---------|
| 生产环境 | 1/100000 | 1M records/s | < 2% |
| 测试环境 | 1/1000 | 100K records/s | < 5% |
| 开发环境 | 1/100 | 10K records/s | < 10% |

## 分析工具

独立的分析工具生成 HTML 报告，包含：

- 端到端延迟分析
- 各阶段耗时统计
- 性能瓶颈识别
- 时间线可视化

## 故障排查

### 没有生成追踪文件

1. 检查引擎级和任务级开关是否都已启用
2. 验证 `stain-trace-file-base-path` 目录权限
3. 查看日志中的 "StainTrace" 或 "TraceFileWriter" 消息

### 空的追踪文件

空文件是正常现象 - 它们是为文件轮转预先创建的，如果作业提前结束可能未使用。可以安全删除。

### 只有部分阶段数据

这种情况发生在 Transform 将 1 条记录分裂为 N 条记录时。默认情况下，只有第一条输出继承追踪负载。设置 `stain-trace-propagate-to-all-splits: true` 可追踪所有分裂。

## 相关文档

- [快速开始指南](../../../seatunnel-trace/STAIN_TRACE_QUICKSTART.md)
- [事件监听器](../event-listener.md)
- [遥测](telemetry.md)
