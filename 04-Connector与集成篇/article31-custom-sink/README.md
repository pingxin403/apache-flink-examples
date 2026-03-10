# Article 31: 自定义 Sink 开发 —— TwoPhaseCommitSinkFunction 实现事务写入

本示例演示如何通过 `TwoPhaseCommitSinkFunction` 实现一个支持 Exactly-Once 语义的自定义文件 Sink。

## 功能说明

- 实现 `TransactionalFileSink`，基于两阶段提交协议保证 Exactly-Once 写入
- 数据先写入 `.tmp` 临时文件，Checkpoint 成功后重命名为 `.data` 正式文件
- 故障恢复时自动回滚未提交的事务（删除临时文件）
- 支持并行执行，每个子任务独立管理自己的事务文件

## 核心特性

### 1. 两阶段提交（2PC）
- `beginTransaction()`: 创建临时文件，打开写入流
- `invoke()`: 将数据追加写入临时文件
- `preCommit()`: flush + close，确保数据持久化到磁盘
- `commit()`: 原子重命名 `.tmp` → `.data`，数据对外可见
- `abort()`: 关闭流并删除临时文件，回滚事务

### 2. Exactly-Once 保证
- 事务句柄（文件路径）保存在 Checkpoint 中
- 故障恢复后，对已预提交的事务重新执行 commit
- commit 操作幂等：临时文件不存在则跳过

### 3. 事务句柄设计
- `FileTransaction` 实现 `Serializable`
- `BufferedWriter` 标记为 `transient`，不参与序列化
- 通过文件路径在恢复时重建写入流

## 前置条件

- Flink 版本 >= 1.17
- Java 11+
- Maven 3.6+

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例

```bash
flink run -c com.example.flink.CustomSinkExample \
  target/article31-custom-sink-1.0-SNAPSHOT.jar
```

### 3. 观察输出

查看输出目录 `/tmp/flink-sink-output`：

```bash
# 实时观察文件变化
watch -n 1 'ls -la /tmp/flink-sink-output/'
```

你会看到：
- `.tmp` 文件：正在写入的数据（预提交阶段）
- `.data` 文件：已提交的数据（Checkpoint 成功后）

### 4. 预期输出

```
/tmp/flink-sink-output/
├── data-0-1704067200000.data    # 子任务 0 的已提交数据
├── data-1-1704067200000.data    # 子任务 1 的已提交数据
├── tmp-0-1704067210000.tmp      # 子任务 0 正在写入的数据
└── tmp-1-1704067210000.tmp      # 子任务 1 正在写入的数据
```

每个 `.data` 文件内容示例：

```json
{"deviceId":"device-0-0","temperature":36.7,"timestamp":"2024-01-01 14:30:05","seq":0}
{"deviceId":"device-0-1","temperature":28.3,"timestamp":"2024-01-01 14:30:07","seq":1}
{"deviceId":"device-0-2","temperature":42.1,"timestamp":"2024-01-01 14:30:09","seq":2}
```

## 项目结构

```
article31-custom-sink/
├── pom.xml                          # Maven 依赖配置
├── README.md                        # 项目说明
└── src/main/java/com/example/flink/
    ├── FileTransaction.java          # 事务句柄（封装临时/目标文件路径）
    ├── TransactionalFileSink.java    # 两阶段提交 Sink 实现
    └── CustomSinkExample.java        # 主程序入口
```

## 配置说明

### Checkpoint 配置

| 参数 | 示例值 | 生产推荐值 | 说明 |
|:-----|:------:|:----------:|:-----|
| `checkpointInterval` | 10000 | 60000 | Checkpoint 间隔（毫秒） |
| `checkpointingMode` | EXACTLY_ONCE | EXACTLY_ONCE | 精确一次语义 |
| `checkpointTimeout` | 60000 | 600000 | 超时时间（毫秒） |
| `minPauseBetweenCheckpoints` | 3000 | 5000 | 两次 Checkpoint 最小间隔 |

### 2PC 方法调用时序

```
作业启动 → beginTransaction()
  ↓
数据到达 → invoke() → invoke() → invoke() ...
  ↓
Checkpoint 触发 → preCommit() → 保存事务到 Checkpoint → beginTransaction()
  ↓
Checkpoint 成功 → commit()
  ↓
继续处理 → invoke() → invoke() ...
```

## 常见问题

### 1. 为什么 .tmp 文件没有变成 .data？

检查 Checkpoint 是否正常完成。只有 Checkpoint 成功后才会触发 commit。
可以在 Flink Web UI 中查看 Checkpoint 状态。

### 2. 故障恢复后数据会重复吗？

不会。未提交的 `.tmp` 文件会被 abort 删除，已提交的 `.data` 文件不受影响。
从 Checkpoint 恢复后，数据会从断点重新处理，写入新的事务文件。

### 3. 并行度变化后会有问题吗？

每个子任务独立管理自己的事务文件（文件名包含 subtaskIndex），
并行度变化不会导致文件冲突。

## 参考资料

- [Flink 官方文档：TwoPhaseCommitSinkFunction](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/guarantees/)
- [Flink 官方文档：Checkpoint 机制](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)
- [两阶段提交协议](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)

## 相关文章

- [第 24 篇：Kafka + Flink Exactly-Once](../../../Flink/04-Connector与集成篇/24-Kafka+Flink%20Exactly-Once.md)
- [第 30 篇：自定义 Source 开发](../../../Flink/04-Connector与集成篇/30-自定义Source开发.md)
- [第 31 篇：自定义 Sink 开发](../../../Flink/04-Connector与集成篇/31-自定义Sink开发.md)
