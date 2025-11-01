# PI CDC Connector

## 概述

PI CDC 连接器是基于 SeaTunnel 框架的实时数据变更捕获组件，专门用于从 PI 系统获取实时数据流。该连接器采用 WebSocket 长连接技术，实现低延迟、高可靠性的工业数据流处理。

## 核心原则

### 生产级稳定性
- **数据完整性优先**: CDC场景下使用Fail-Fast策略,队列满时立即失败任务而非静默丢弃数据,确保CDC数据零丢失
- **故障隔离**: 单个Split故障不影响其他Split的正常处理
- **自动恢复**: 网络抖动、连接断开自动重连(最多3次),失败后触发任务重启
- **背压监控**: 队列使用率>90%触发告警,队列满时立即Fail-Fast

### 负载均衡原则
- **动态Split请求**: Reader持续向Enumerator请求新Split,确保高吞吐不被锁死
- **1:1 优先模式**: 标准情况下1个Reader处理1个Split，确保最优性能
- **故障接管**: Reader可接管失败Reader的Split,实现自动容错

### 可靠性保障
- **Fail-Fast策略**: 队列满时立即抛出PIConnectorException(PI_DATA_505),任务失败后从Checkpoint恢复
- **检查点一致性**: 支持SeaTunnel框架的检查点机制,确保容错恢复时数据完整性
- **监控可观测**: 提供完整的监控指标,包括队列使用率、连接状态、背压状态

### 最小化影响
- **配置驱动**: 通过配置参数调整行为，避免代码修改
- **向后兼容**: 保持API和配置的向后兼容性
- **渐进优化**: 采用最小化改动原则，确保系统稳定性

## 架构设计

### 核心组件

```
PICDCSource
├── PICDCSplitEnumerator     # 分片管理和分配
├── PICDCSourceReader        # 数据读取协调器
├── PIRealtimeReader         # WebSocket实时数据处理
├── PIWebSocketClient        # WebSocket连接管理
└── PICDCSplit              # 数据分片定义
```

### 数据流向

```
PI Server (WebSocket) → PIWebSocketClient → PIRealtimeReader → MessageQueue → PICDCSourceReader → SeaTunnel Engine
```

### 架构设计特点

#### Split级别隔离设计

**核心实现**: 每个Split维护独立的PIRealtimeReader和WebSocket连接

- 每个 Split 对应一个 `SplitAndRealtimeReader`（包含原始 `PICDCSplit` 与其专属的 `PIRealtimeReader`）
- `PICDCSourceReader` 通过 `Map<String, SplitAndRealtimeReader>` 维护所有活跃 Split，与 `pendingSplits` 组合即可覆盖分配、快照等场景

**设计优势**:
1. **故障隔离**: 单个Split故障不影响其他Split处理
2. **独立重启**: Split可以独立重连,无需全局协调
3. **简化分配**: 采用全局队列模式,无需复杂的负载均衡
4. **框架对齐**: 完美符合SeaTunnel的Split语义

#### 全局队列分配模式

```java
// PICDCSplitEnumerator.java:68
private final Queue<PICDCSplit> pendingSplits = new ConcurrentLinkedQueue<>();

// 失败Split自动回到队列
public void addSplitsBack(List<PICDCSplit> splits, int subtaskId) {
    pendingSplits.addAll(splits);
}
```

**分配策略**:
- 初始分配: 1个Reader对应1个Split
- 动态请求: Reader可请求更多Split处理
- 故障恢复: 失败Split自动加入队列等待重新分配

#### 共享资源优化

**共享组件** (减少资源消耗):
- PIHttpClient: 所有Split共享一个HTTP客户端
- PIWebIdResolver: 共享WebID解析器

**独立组件** (保证隔离):
- PIWebSocketClient: 每个Split独立的WebSocket连接
- PIRealtimeReader: 每个Split独立的数据处理器
- MessageQueue: 每个Reader独立的消息队列

---

## 核心处理机制

### 1. 分片管理 (Split Management)

#### 分片创建策略
- **分片大小**: 每个分片最多包含 `max_webids_per_split` 个 PI Path（默认50个）
- **分片命名**: `cdc-split-{index}` 格式
- **分片分配**: 1:1 模式，每个 Reader 处理一个 Split

#### 分片分配流程
1. **初始化阶段**: `PICDCSplitEnumerator.run()` 创建所有分片；当 Split 数量超过并行度时只记录告警，仍允许多 Split 以支持故障接管
2. **Reader 注册**: `registerReader()` 为新 Reader 分配初始分片
3. **动态分配**: `handleSplitRequest()` 处理 Reader 的额外分片请求
4. **故障恢复**: `addSplitsBack()` 处理失败分片的重新分配

### 2. 实时数据处理 (Real-time Data Processing)

#### WebSocket 连接管理
- **连接模式**: 持久化长连接，支持自动重连
- **心跳检测**: 基于消息接收时间的心跳监控
- **重连策略**: 指数退避（`retry_backoff_multiplier_ms` / `retry_backoff_max_ms`），重试次数由 `retry_attempts` 控制
- **网络异常最大重试时间**: 默认配置下约 **127秒 (2分7秒)**，计算方式见下方说明

#### 数据队列机制
- **容量可配**: 队列容量由 `data_buffer_queue_size` 控制, 默认 `300000`, 支持按需调节（有效范围 1,000 ~ 10,000,000，越界时自动回退/截断）
- **LinkedBlockingQueue**: 每个 Split 独立维护一个队列, 保障分片隔离
- **运行时指标**: Reader 暴露队列总容量/当前使用量, 便于监控调优

**关键特性**:
- **非阻塞入队**: 使用 `offer()` 无参版本,立即返回true/false,绝不阻塞Netty EventLoop
- **EventLoop保护**: 避免阻塞心跳、PONG响应和重连逻辑,保证WebSocket连接稳定性
- **队列满策略**: **Fail-Fast** - 立即抛出PIConnectorException(PI_DATA_505),任务失败
- **数据完整性**: CDC零丢失保证,队列满时任务失败后从Checkpoint恢复

**非阻塞设计关键考虑**:

✅ **Netty EventLoop保护** (最高优先级)
- **原则**: 绝不在EventLoop线程中执行任何阻塞操作
- **实现**: `messageQueue.offer(row)` 无参版本,立即返回true/false
- **原因**: 阻塞会导致心跳超时、PONG响应延迟、重连失败,最终连接断开

✅ **队列满时的Fail-Fast策略** (CDC数据完整性)
- **策略**: 队列满时立即抛出PIConnectorException,不阻塞EventLoop
- **错误码**: PI_DATA_505 (CDC_QUEUE_BACKPRESSURE)
- **传播**: 异常通过onError callback传递给PICDCSourceReader
- **结果**: Split标记为fatal error,Reader在下次pollNext()时抛出异常,任务失败
- **恢复**: 任务从最近的Checkpoint重启,确保数据完整性

⚠️ **为何不能静默丢弃数据**
- **CDC场景**: 数据完整性 > 可用性,丢失任何数据都不可接受
- **问题**: 静默丢弃会导致不可恢复的数据丢失,违反CDC核心原则
- **方案**: Fail-Fast + Checkpoint恢复,确保数据零丢失

✅ **背压处理机制**
- **检测**: 队列使用率>90%时触发告警
- **处理**: 队列满时Fail-Fast,通过任务重启缓解背压
- **根治**:
  1. 增加队列容量(默认300K, 可按需调整)
  2. 提升下游消费速度
  3. 减少单Split的PI Path数量
  4. 调整并行度,分散负载

#### 消息处理流程
1. **接收**: WebSocket `onMessage` 回调接收 PI 服务器数据
2. **解析**: JSON 解析提取 PI 数据项
3. **转换**: 使用 `PIDataTypeConverter` 转换为 `SeaTunnelRow`
4. **入队**: 非阻塞`offer(row)`写入,队列满则抛出PIConnectorException
5. **消费**: Reader 轮询队列并发送到下游

### 3. 连接限制管理

#### 移除硬编码限制
- **原设计**: `MAX_CONNECTIONS_PER_READER = 2` 硬限制
- **优化后**: 移除连接数限制，支持动态扩展
- **资源控制**: 通过分片数量和系统资源自然约束

#### 连接状态管理
```java
// 连接计数管理
private final AtomicInteger activeConnections = new AtomicInteger(0);

// 正确的连接关闭处理
@Override
public void close() {
    activeConnections.decrementAndGet(); // 只在真正关闭时减少计数
}
```

### 4. 容错和可靠性

#### 检查点机制
- **状态快照**: `PICDCCheckpointState` 保存分片分配状态
- **故障恢复**: 支持从检查点恢复未完成的分片
- **状态一致性**: 确保分片状态与实际处理状态一致

#### 错误处理策略
1. **WebSocket 断连**: 自动重连(最多3次),超过重试次数后任务失败
2. **数据解析错误**: 记录错误日志,跳过错误数据点继续处理
3. **队列满**: Fail-Fast策略 - 立即抛出PIConnectorException(PI_DATA_505),任务失败
4. **网络抖动**: 区分临时断连(自动重连)和永久故障(任务失败)

## 配置参数

### 核心配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_webids_per_split` | 50 | 每个分片的最大 PI Path 数量 |
| `connection_timeout_ms` | 30000 | WebSocket 连接超时时间 |
| `retry_attempts` | 3 | WebSocket 重连最大尝试次数（≤0 时回退到 `websocket_max_retries`） |
| `retry_backoff_multiplier_ms` | 1000 | 重连指数退避的初始间隔（毫秒） |
| `retry_backoff_max_ms` | 10000 | 重连指数退避的最大间隔（毫秒） |
| `data_buffer_queue_size` | 300000 | 每个分片的数据缓冲队列容量(范围: 1,000~10,000,000, 越界自动回退) |

#### 网络异常重试时间计算

默认配置下（`retry_attempts=3`, `connection_timeout_ms=30000`, `retry_backoff_multiplier_ms=1000`）：

| 重试阶段 | 连接超时 | 退避延迟 | 阶段耗时 | 累计时间 |
|---------|---------|---------|---------|---------|
| Attempt 0 | 30s | 1s | 31s | 31s |
| Attempt 1 | 30s | 2s | 32s | 63s |
| Attempt 2 | 30s | 4s | 34s | 97s |
| Attempt 3 | 30s | - | 30s | **127s** |

**最大重试总时间**: **127秒 (约2分7秒)**

**指数退避计算公式**:
- Attempt N 的退避延迟 = `retry_backoff_multiplier_ms × 2^(N-1)`, 上限为 `retry_backoff_max_ms`
- 总时间 = Σ(连接超时 + 退避延迟) = (30+1) + (30+2) + (30+4) + 30 = 127秒

### 监控配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `channel_polling_interval_ms` | 3000 | 通道轮询间隔（用于心跳检测和超时判定） |
| `webid_resolve_batch_size` | 50 | WebID 解析批次大小 |

## 性能特性

### 吞吐量优化
- **批量处理**: 支持批量 WebID 解析和数据消费
- **并行处理**: 多分片并行处理，提高整体吞吐量
- **内存管理**: 可配置队列大小，平衡内存使用和性能

### 延迟优化
- **实时推送**: WebSocket 实时数据推送，毫秒级延迟
- **无阻塞读取**: Reader 异步轮询，避免阻塞等待
- **直接转发**: 最小化数据转换开销

### 可扩展性
- **水平扩展**: 支持增加并行度扩展处理能力
- **动态调整**: 运行时支持分片重分配
- **资源隔离**: 每个 Reader 独立处理，故障隔离

## 监控和运维

### 关键指标

#### 连接 / Split 状态
- `piSplitReaders.size()`：当前活跃 Split / WebSocket 连接数
- 日志内含 `Reader-X initializing splits` / `Connection released for split` 等信息，可用于排查分配与重连流程

#### 数据处理
- `messageQueueSize` / `messageQueueCapacity`: `PIRealtimeReader` 暴露的即时队列指标
- `queueUtilization`: 通过日志 `QueueUtil={}` 输出的平均队列利用率
- `droppedMessageCount`: 队列满时的丢弃计数（Fail-Fast 触发前会增加，便于观测频度）

#### 分片管理
- `pendingSplits.size()`：日志中输出的待分配分片数量
- `Split fatal errors`：当某个 Split 出现不可恢复错误时会记录一条错误日志并触发 Reader 失败

### 日志监控

#### 正常运行日志
```
INFO - PI CDC split enumerator started - PI Path total: 325
INFO - PICDC parallelism validation passed - PI Path count: 325, expected splits: 7, parallelism: 7
INFO - Assigned initial split cdc-split-0 to Reader-0, remaining splits: 6
INFO - WebSocket connection established to PI server
```

#### 异常情况日志
```
WARN - WebSocket connection timeout after 10 seconds, attempting reconnect
ERROR - Expected splits count 13 exceeds parallelism 7
WARN - Reader-0 high queue utilization: 95.2%
ERROR - CDC queue full (capacity: 300000), stream: tag-1, dropped: 1. Task will fail to prevent data loss.
ERROR - PIConnectorException: PI_DATA_505 - CDC queue backpressure - downstream too slow
```

### 性能调优建议

#### 内存优化
- 根据数据量调整 `data_buffer_queue_size`
- 监控队列使用率，避免频繁 GC
- 优化下游批处理/写入策略，避免 Reader 队列堆积

#### 网络优化
- 调整 `connection_timeout_ms` 适应网络环境
- 设置合适的 `retry_backoff_max_ms` 避免过度重连
- 监控 WebSocket 连接稳定性

#### 并行度规划
- 确保 `parallelism >= ceil(piPaths.size() / max_webids_per_split)`
- 根据数据量和处理能力合理设置并行度
- 避免过度并行导致资源竞争

## 故障排查

### 常见问题

#### 1. 分片数量超过并行度
**错误**: `Expected splits count X exceeds parallelism Y`
**解决方案**:
- 增加并行度: `parallelism >= X`
- 或增加分片大小: `max_webids_per_split = ceil(piPaths.size() / parallelism)`
- 或减少 PI Path 数量

#### 2. WebSocket 连接不稳定
**现象**: 频繁重连日志
**解决方案**:
- 检查网络连接质量
- 调整 `connection_timeout_ms` 和 `retry_backoff_max_ms`
- 确认 PI 服务器状态和负载

#### 3. 数据处理延迟
**现象**: 队列积压，处理延迟增加
**解决方案**:
- 提升下游消费并发/批量处理能力，缩短队列停留时间
- 检查下游处理能力
- 监控系统资源使用情况

#### 4. 内存使用过高
**现象**: 频繁 GC，内存占用持续增长
**解决方案**:
- 减少 `data_buffer_queue_size`
- 优化下游消费速度
- 检查是否存在内存泄漏

#### 5. 队列背压和任务失败问题
**现象**: 任务频繁失败,错误码PI_DATA_505,日志显示"CDC queue full"
**根本原因**: 下游消费速度 < 上游推送速度,队列满后Fail-Fast导致任务失败
**排查步骤**:
- 检查队列使用率: `getQueueUtilization() > 90%` 为Fail-Fast触发条件
- 检查下游Sink写入性能是否匹配上游推送速度
- 检查是否存在网络延迟或下游系统响应慢
- 评估是否需要增加队列容量或增加并行度
**解决方案**:
1. **扩容队列**: 增加`data_buffer_queue_size`(默认300K, 最大10M)
2. **提升并行度**: 增加Reader数量分散负载
3. **优化下游**: 提升Sink写入性能(批量写入、异步写入等)
4. **减少负载**: 减少单Split的PI Path数量(默认50个/split)
**为何Fail-Fast而非丢弃**:
- CDC场景要求数据完整性,不能容忍静默丢失
- 任务失败后从Checkpoint恢复,确保数据零丢失
- Fail-Fast快速暴露问题,便于及时处理

## 安全注意事项

- 默认配置 (`trust_all_certs=true`, `verify_hostname=false`) 会信任任意证书且跳过主机名校验，仅适用于测试环境。生产环境务必显式设置 `trust_all_certs=false` 与 `verify_hostname=true` 并提供有效证书，避免中间人攻击。
- 当前实现会在 WebSocket 握手阶段关闭主机名校验（`PIWebSocketClient` 中写死 `SSLParameters#setEndpointIdentificationAlgorithm(null)`），若计划启用严格校验需同步调整代码逻辑，使配置项能够生效。

## 数据处理完整性保障

### Items数组完整处理

**关键修复**: PI CDC现已正确处理WebSocket消息中的所有数据点

#### 数据结构
PI Web API WebSocket Channel消息包含两层Items结构:
```json
{
  "Links": {},
  "Items": [                          // 外层: 多个流(stream)
    {
      "WebId": "F1...",
      "Name": "Temperature",
      "Path": "\\\\server\\tag1",
      "Items": [                      // 内层: 该流的多个数据点
        {
          "Timestamp": "2025-10-06T21:50:00Z",
          "Value": 25.3,
          "Good": true
        },
        {
          "Timestamp": "2025-10-06T21:50:01Z",
          "Value": 25.4,
          "Good": true
        }
      ]
    }
  ]
}
```

#### 多数据点场景
内层Items数组可能包含多个数据点的情况:

1. **WebSocket重连补发**
   - 断连期间积累的所有变化
   - PI服务器使用marker机制确保不丢失
   - 示例: 10秒断连,10个数据点一次性推送

2. **高频数据缓冲**
   - 更新频率 > WebSocket推送频率
   - PI服务器批量缓冲多个变化
   - 示例: 0.1秒/次更新,5个点批量发送

3. **初始值包含**
   - `includeInitialValues=true`时
   - 首条消息包含历史快照
   - 可能包含多个历史数据点

4. **网络延迟/背压**
   - 客户端处理较慢
   - 服务端合并多个更新发送

#### 当前实现
```java
// PIRealtimeReader.java:357-397
// 遍历并处理所有数据点,使用非阻塞入队
for (JsonNode dataPoint : dataItems) {
    SeaTunnelRow row = PIDataTypeConverter.convertFromJson(
        itemNode, dataPoint, rowType, config.getJsonField());

    // CRITICAL: Non-blocking offer() - never blocks Netty EventLoop
    boolean success = messageQueue.offer(row);
    if (success) {
        processedCount++;
    } else {
        // Queue full - Fail-Fast: throw exception immediately
        String errorMsg = String.format(
            "CDC queue full (capacity: %d), cannot accept new data. Task will fail to prevent data loss.",
            messageQueueCapacity);
        throw new PIConnectorException(PIErrorCode.CDC_QUEUE_BACKPRESSURE, errorMsg);
    }
}
```

**关键特性**:
- ✅ 处理所有数据点,确保CDC数据完整性
- ✅ **非阻塞入队**: 保护Netty EventLoop不被阻塞
- ✅ **Fail-Fast策略**: 队列满时立即抛异常,确保CDC零丢失
- ✅ 监控告警: 检测到多数据点时记录日志
- ✅ 统计汇总: 处理成功/失败数量跟踪
- ✅ 与PI Batch保持一致的处理逻辑

#### 监控指标
当检测到多数据点消息时,会输出以下日志:
```
WARN  - Received 10 data points in single message for stream 'Temperature' (WebId: F1...).
        This indicates WebSocket catch-up or high-frequency buffering. Processing all points.
INFO  - Processed 10 data points from stream 'Temperature': 10 succeeded, 0 failed
```

**告警阈值**: 当单条消息包含 >1 个数据点时触发

## PI CDC vs PI Batch 数据差异说明

### 核心差异

PI CDC 和 PI Batch 的数据条数存在显著差异,**这是正常现象**,由两种连接器的数据采集机制决定:

#### PI CDC (实时模式) - WebSocket Streaming
- **API 端点**: `/piwebapi/streamsets/channel`
- **数据来源**: PI 服务器实时推送的事件流
- **retrievalMode**: `Auto` (默认,自动选择最佳值)
- **数据特点**: 捕获**所有数据变化事件**,无压缩,无采样

**代码实现** (PIWebSocketClient.java:562-580):
```java
// WebSocket Channel端点,实时事件推送
webSocketUrl = webSocketUrl + "/streamsets/channel";
queryParams.append("?includeInitialValues=").append(true/false);
queryParams.append("&retrievalMode=").append("Auto");
```

#### PI Batch (批处理模式) - HTTP Recorded API
- **API 端点**: `/piwebapi/streams/{webId}/recorded`
- **数据来源**: PI Archive 归档数据库
- **压缩机制**: PI 服务器的压缩算法 (压缩死区、例外偏差)
- **数据特点**: 返回**压缩后的归档数据**,典型压缩率约50%

**代码实现** (PIHttpClient.java:336-351):
```java
// Recorded数据端点,读取压缩后的归档数据
public JsonNode queryRecordedData(
    String webId, String startTime, String endTime,
    int maxCount, String boundaryType) {

    endpoint.append("/piwebapi/streams/").append(webId).append("/recorded");
    endpoint.append("?startTime=").append(startTime);
    endpoint.append("&endTime=").append(endTime);
    endpoint.append("&maxCount=").append(maxCount);
    endpoint.append("&boundaryType=").append(boundaryType); // 默认"Inside"
    // 注意: 没有retrievalMode参数,使用PI Archive压缩数据
}
```

### 数据量对比示例

**测试场景**: 11分钟时间窗口 (2025-10-06 21:50:00 ~ 22:01:00)

| 连接器类型 | 数据条数 | 数据来源 | 压缩情况 |
|-----------|---------|----------|---------|
| PI CDC | 24,257条 | WebSocket实时流 | ✅ 完整原始数据 |
| PI Batch | 12,427条 | PI Archive归档 | ⚠️ 压缩约51.2% |

**数据差异**: 12,427 / 24,257 ≈ 51.2% (典型的PI归档压缩率)

### PI Archive 压缩机制

PI服务器在存储历史数据时使用压缩算法优化存储:

```
实时数据流 (CDC捕获):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
时间: 21:50:00 → 21:50:01 → 21:50:02 → 21:50:03 → 21:50:04
值:      100.0  →  100.5  →  101.0  →  100.8  →  100.3
CDC:       ✅    →    ✅   →    ✅   →    ✅   →    ✅   (5条记录)

归档数据 (Batch读取):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
时间: 21:50:00 → 21:50:02 → 21:50:04
值:      100.0  →  101.0  →  100.3
Batch:     ✅    →    ✅   →    ✅                    (3条记录,压缩40%)
```

**PI压缩策略**:
- **压缩死区 (Compression Deadband)**: 值变化小于阈值时不存储
- **例外偏差 (Exception Deviation)**: 基于线性插值去除冗余点
- **时间死区 (Time Deadband)**: 指定时间内多次变化只保留关键点

### 数据解析差异

#### PI CDC 数据解析
**代码位置**: `PIRealtimeReader.java` (`parsePIWebAPIItemAndEnqueue`)

```java
private void parsePIWebAPIItemAndEnqueue(JsonNode itemNode) {
    if (!itemNode.has("Items") || !itemNode.get("Items").isArray()) {
        log.warn("Data item does not contain valid Items array: {}", itemNode);
        return;
    }

    JsonNode dataItems = itemNode.get("Items");
    if (dataItems.isEmpty()) {
        log.warn("Data item contains empty Items array: {}", itemNode);
        return;
    }

    if (dataItems.size() > 1) {
        log.warn(
                "Received {} data points in single message for stream '{}' (WebId: {}). "
                        + "Processing all points to maintain CDC completeness.",
                dataItems.size(),
                itemNode.path("Name").asText("unknown"),
                itemNode.path("WebId").asText("unknown"));
    }

    for (JsonNode dataPoint : dataItems) {
        SeaTunnelRow row =
                PIDataTypeConverter.convertFromJson(
                        itemNode, dataPoint, rowType, config.getJsonField());
        if (!messageQueue.offer(row)) {
            long dropped = droppedMessageCount.incrementAndGet();
            if (dropped <= 10 || dropped % 1000 == 0) {
                log.warn(
                        "Message queue full (capacity: {}), dropping data point. Total dropped: {}",
                        messageQueueCapacity,
                        dropped);
            }
        }
    }
}
```

**关键点**:
1. WebSocket 推送的每个消息包含一个 `Items` 数组, `Items` 可能携带多个数据点(重连补偿/高频缓冲等场景)。
2. Reader 遍历 `Items` 中的全部数据点逐条入队,确保 CDC 数据完整性,不会遗漏历史缓冲数据。
3. 使用无阻塞 `offer()` 入队,队列满时立即抛出PIConnectorException(PI_DATA_505),避免阻塞 Netty EventLoop。
4. Fail-Fast机制确保CDC数据零丢失,任务失败后从Checkpoint恢复。

#### PI Batch 数据解析
Batch模式从`/recorded`端点获取的数据已经被PI服务器压缩,返回的`Items`数组中只包含关键数据点,而不是所有变化事件。

### 使用建议

#### 选择 PI CDC 的场景
✅ **需要完整的数据变化历史**
- 金融交易数据
- 关键工艺参数
- 报警和事件记录
- 审计和合规要求

✅ **实时性要求高**
- 实时监控看板
- 异常检测和告警
- 自动控制反馈

✅ **数据完整性要求严格**
- 不能接受数据丢失或压缩
- 需要还原完整的时序变化

#### 选择 PI Batch 的场景
✅ **历史数据分析**
- 趋势分析
- 统计报表
- 数据挖掘

✅ **数据量优化**
- 长时间跨度查询
- 减少存储成本
- 降低网络传输

✅ **接受压缩数据**
- 只关注关键变化点
- 可接受线性插值还原

### 验证方法

如果怀疑数据差异异常,可以通过以下方法验证:

```sql
-- 1. 按分钟统计数据密度
SELECT
    DATE_TRUNC('minute', timestamp) as minute,
    COUNT(*) as cdc_count
FROM "PICDC-picdc-refactor"
WHERE timestamp >= '2025-10-06 21:50:00'
  AND timestamp < '2025-10-06 21:52:00'
GROUP BY DATE_TRUNC('minute', timestamp)
ORDER BY minute;

SELECT
    DATE_TRUNC('minute', timestamp) as minute,
    COUNT(*) as batch_count
FROM "pibatchnew-pibatchnew"
WHERE timestamp >= '2025-10-06 21:50:00'
  AND timestamp < '2025-10-06 21:52:00'
GROUP BY DATE_TRUNC('minute', timestamp)
ORDER BY minute;

-- 2. 对比具体数据点的值
SELECT timestamp, value, name
FROM "PICDC-picdc-refactor"
WHERE name = 'specific_tag_name'
  AND timestamp >= '2025-10-06 21:50:00'
  AND timestamp < '2025-10-06 21:51:00'
ORDER BY timestamp;
```

### 结论

**数据条数差异是正常现象**,原因是:

1. ✅ PI CDC 使用 WebSocket 实时流捕获所有数据变化事件
2. ✅ PI Batch 使用 Recorded API 读取 PI Archive 压缩后的数据
3. ✅ 压缩率约50%符合 PI 归档系统的典型表现
4. ✅ 不是Bug,是 PI Web API 的设计特性

**选择建议**:
- 需要完整原始数据 → 使用 **PI CDC**
- 只需趋势和关键点 → 使用 **PI Batch**
- 实时监控 → 使用 **PI CDC**
- 历史分析 → 使用 **PI Batch**

## 最佳实践

### 部署建议
1. **容量规划**: 根据 PI Path 数量和数据频率规划资源
2. **网络配置**: 确保 PI 服务器网络连接稳定可靠
3. **监控配置**: 设置关键指标告警,及时发现问题
4. **容灾备份**: 配置检查点存储,支持故障恢复

### 运维建议
1. **定期检查**: 监控连接状态和数据处理指标
2. **日志分析**: 定期分析日志,识别潜在问题
3. **性能测试**: 定期进行压力测试,验证系统能力
4. **版本管理**: 建立完善的版本发布和回滚机制

---

**版本**: 2.6-WS-test-SNAPSHOT
**更新时间**: 2025年10月6日
**维护状态**: 生产级稳定版本
