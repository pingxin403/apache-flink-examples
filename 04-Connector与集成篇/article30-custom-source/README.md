# Article 30: 自定义 Source 开发 —— 从零实现一个生产级数据源

本示例演示如何开发自定义 Flink Source，包括 Checkpoint 集成、状态恢复和并行度控制。

## 功能说明

- 实现自定义 `HttpPollingSource`，定时轮询 HTTP 接口获取数据
- 通过 `CheckpointedFunction` 集成 Checkpoint，记录偏移量
- 故障恢复时从上次 Checkpoint 的偏移量继续消费
- 支持并行执行，每个实例根据 subtaskIndex 请求不同的数据分片
- 提供 `SimulatedHttpPollingSource` 用于本地测试（无需真实 HTTP 服务）

## 核心特性

### 1. Checkpoint 集成
- `initializeState()`: 作业启动或恢复时读取偏移量
- `snapshotState()`: Checkpoint 触发时保存当前偏移量
- `CheckpointLock`: 保证数据发送与状态更新的原子性

### 2. 状态恢复
- 使用 Operator State（ListState）存储偏移量
- 支持 Even-split 重分布策略（并行度变化时均匀分配）
- `context.isRestored()` 判断是否为故障恢复场景

### 3. 并行度控制
- 通过 `getRuntimeContext().getIndexOfThisSubtask()` 获取实例索引
- 每个实例只处理属于自己的数据分片，避免重复消费

### 4. 生产级特性
- HTTP 请求超时控制
- 指数退避重试机制
- 正确处理 `InterruptedException`，确保 cancel 后及时退出

## 前置条件

- Flink 版本 >= 1.17
- Java 11+
- Maven 3.6+

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例（使用模拟 Source）

```bash
flink run -c com.example.flink.CustomSourceExample \
  target/article30-custom-source-1.0-SNAPSHOT.jar
```

### 3. 预期输出

```
Output:1> [14:30:05] {"deviceId":"device-0-0","temperature":36.7,"timestamp":"2024-01-01T06:30:05Z","offset":0}
Output:2> [14:30:05] {"deviceId":"device-1-0","temperature":28.3,"timestamp":"2024-01-01T06:30:05Z","offset":0}
Output:1> [14:30:07] {"deviceId":"device-0-1","temperature":42.1,"timestamp":"2024-01-01T06:30:07Z","offset":1}
Output:2> [14:30:07] {"deviceId":"device-1-1","temperature":31.5,"timestamp":"2024-01-01T06:30:07Z","offset":1}
```

### 4. 使用真实 HTTP Source

将 `CustomSourceExample.java` 中的 Source 替换为：

```java
DataStream<String> sourceStream = env
        .addSource(new HttpPollingSource(
                "http://your-api.com/data",  // HTTP 接口地址
                5000,                         // 轮询间隔 5 秒
                3000                          // HTTP 超时 3 秒
        ))
        .setParallelism(4)
        .name("HTTP Polling Source");
```

## 项目结构

```
article30-custom-source/
├── pom.xml                          # Maven 依赖配置
├── README.md                        # 项目说明
└── src/main/java/com/example/flink/
    ├── HttpPollingSource.java        # 自定义 HTTP 轮询 Source（生产级）
    ├── SimulatedHttpPollingSource.java # 模拟 Source（本地测试用）
    └── CustomSourceExample.java      # 主程序入口
```

## 配置说明

### Checkpoint 配置

| 参数 | 示例值 | 生产推荐值 | 说明 |
|:-----|:------:|:----------:|:-----|
| `checkpointInterval` | 10000 | 60000 | Checkpoint 间隔（毫秒） |
| `checkpointingMode` | EXACTLY_ONCE | EXACTLY_ONCE | 精确一次语义 |
| `checkpointTimeout` | 60000 | 600000 | 超时时间（毫秒） |
| `minPauseBetweenCheckpoints` | 3000 | 5000 | 两次 Checkpoint 最小间隔 |

### HttpPollingSource 配置

| 参数 | 说明 | 推荐值 |
|:-----|:-----|:------:|
| `url` | HTTP 接口地址 | — |
| `intervalMs` | 轮询间隔（毫秒） | >= 1000 |
| `httpTimeoutMs` | HTTP 超时（毫秒） | < Checkpoint 间隔的 1/3 |

## 常见问题

### 1. Checkpoint 超时

如果 HTTP 请求耗时过长，会阻塞 CheckpointLock，导致 Checkpoint 超时。
解决方案：设置合理的 `httpTimeoutMs`，确保小于 Checkpoint 间隔的 1/3。

### 2. 并行度变化后数据重复

使用 Even-split 模式时，并行度变化会导致状态重新分配。
建议：在扩缩容时使用 Savepoint，并确保 HTTP 接口支持幂等查询。

### 3. cancel 后作业不停止

确保 `run()` 方法中的所有阻塞操作（sleep、HTTP 请求）都正确处理了中断。

## 参考资料

- [Flink 官方文档：DataStream Sources](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/sources/)
- [FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
- [Flink Checkpoint 机制](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)

## 相关文章

- [第 13 篇：Checkpoint 背后的 Chandy-Lamport 算法](../../../Flink/02-State与容错篇/13-Checkpoint背后的Chandy-Lamport算法.md)
- [第 29 篇：Changelog Stream 是什么](../../../Flink/04-Connector与集成篇/29-Changelog%20Stream是什么.md)
- [第 30 篇：自定义 Source 开发](../../../Flink/04-Connector与集成篇/30-自定义Source开发.md)
