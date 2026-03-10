# Article 11: Operator State 实战

本项目演示 Flink Operator State 的使用，包括 BroadcastState 和 ListState 两种类型。

## 项目结构

```
article11-operator-state/
├── src/main/java/com/example/flink/
│   ├── BroadcastStateExample.java      # BroadcastState 完整示例
│   ├── ListStateExample.java           # ListState 完整示例
│   ├── model/
│   │   ├── ConfigRule.java             # 配置规则类
│   │   ├── DataEvent.java              # 数据事件类
│   │   └── Alert.java                  # 告警类
│   └── source/
│       ├── ConfigRuleSource.java       # 配置规则数据源
│       └── DataEventSource.java        # 数据事件数据源
├── pom.xml
└── README.md
```

## 核心概念

### Operator State vs Keyed State

| 对比维度 | Keyed State | Operator State |
|---------|-------------|---------------|
| 作用范围 | 每个 Key 独立 | 每个算子实例独立 |
| 分区方式 | 按 Key 分区 | 按算子并行度分区 |
| 使用前提 | 必须先 keyBy() | 无需 keyBy() |
| 典型场景 | 用户维度聚合 | Source 偏移量、广播配置 |

### BroadcastState

用于将配置/规则广播给所有并行实例：

```java
// 1. 定义 BroadcastState 描述符
MapStateDescriptor<String, ConfigRule> descriptor = 
    new MapStateDescriptor<>("config", String.class, ConfigRule.class);

// 2. 创建广播流
BroadcastStream<ConfigRule> broadcastStream = 
    configStream.broadcast(descriptor);

// 3. 连接数据流和广播流
dataStream
    .keyBy(DataEvent::getUserId)
    .connect(broadcastStream)
    .process(new KeyedBroadcastProcessFunction<...>() {
        // 处理数据流（只读广播状态）
        @Override
        public void processElement(...) {
            ReadOnlyBroadcastState<String, ConfigRule> state = 
                ctx.getBroadcastState(descriptor);
            // 只能读取
        }
        
        // 处理广播流（可写广播状态）
        @Override
        public void processBroadcastElement(...) {
            BroadcastState<String, ConfigRule> state = 
                ctx.getBroadcastState(descriptor);
            // 可以读写
        }
    });
```

### ListState

用于每个算子实例独立的列表状态：

```java
public class BufferedSource implements CheckpointedFunction {
    private List<String> buffer;
    private ListState<String> checkpointedState;
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        checkpointedState.clear();
        for (String item : buffer) {
            checkpointedState.add(item);
        }
    }
    
    @Override
    public void initializeState(FunctionInitializationContext context) {
        ListStateDescriptor<String> descriptor = 
            new ListStateDescriptor<>("buffer", String.class);
        checkpointedState = context.getOperatorStateStore()
            .getListState(descriptor);
        
        buffer = new ArrayList<>();
        if (context.isRestored()) {
            for (String item : checkpointedState.get()) {
                buffer.add(item);
            }
        }
    }
}
```

## 运行示例

### 1. BroadcastState 示例

演示动态配置更新：

```bash
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.flink.BroadcastStateExample"
```

**功能**：
- 配置流：每 10 秒更新一次规则
- 数据流：每秒生成数据事件
- 根据最新规则过滤数据

**输出示例**：
```
规则已更新：rule-1 (threshold=100.0)
数据通过检查：user-1, value=150.0
数据被过滤：user-2, value=50.0
规则已更新：rule-1 (threshold=200.0)
数据被过滤：user-1, value=150.0
```

### 2. ListState 示例

演示带缓冲的 Source：

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.ListStateExample"
```

**功能**：
- Source 缓冲数据，每 10 条发送一次
- Checkpoint 时保存缓冲区
- 故障恢复时恢复缓冲区

**输出示例**：
```
缓冲区大小：5
缓冲区大小：10
发送批次：[data-1, data-2, ..., data-10]
Checkpoint 1 保存了 3 条数据
```

## 并行度变化测试

### Even-split 重分配

```bash
# 并行度 2 运行
flink run -p 2 target/article11-operator-state-1.0-SNAPSHOT.jar

# 从 Savepoint 恢复，并行度改为 3
flink run -p 3 -s hdfs://path/to/savepoint \
    target/article11-operator-state-1.0-SNAPSHOT.jar
```

**重分配逻辑**：
- 实例 0 的状态：[A, B, C, D]
- 实例 1 的状态：[E, F, G, H]
- 重分配后：
  - 新实例 0：[A, B, C]
  - 新实例 1：[D, E, F]
  - 新实例 2：[G, H]

### Union 重分配

使用 `getUnionListState()` 替代 `getListState()`：

```java
checkpointedState = context.getOperatorStateStore()
    .getUnionListState(descriptor);  // Union 模式
```

**重分配逻辑**：
- 实例 0 的状态：[A, B]
- 实例 1 的状态：[C, D]
- 重分配后：
  - 新实例 0：[A, B, C, D]
  - 新实例 1：[A, B, C, D]
  - 新实例 2：[A, B, C, D]

## 注意事项

### BroadcastState

1. **数据流侧只读**：在 `processElement()` 中只能读取广播状态
2. **广播流不能 keyBy**：广播流必须是非 Keyed 流
3. **状态大小控制**：每个实例都保存完整副本，注意内存占用

### ListState

1. **状态无限增长**：需要手动限制列表大小
2. **重分配策略**：根据场景选择 Even-split 或 Union
3. **Checkpoint 集成**：必须实现 CheckpointedFunction 接口

## 性能优化

### 减少广播频率

```java
// 批量更新规则，而非逐条广播
List<ConfigRule> rules = loadRulesFromDB();
for (ConfigRule rule : rules) {
    ruleState.put(rule.getRuleId(), rule);
}
```

### 状态压缩

```java
// 使用压缩序列化器
MapStateDescriptor<String, ConfigRule> descriptor = 
    new MapStateDescriptor<>(
        "config",
        String.class,
        ConfigRule.class
    );
descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build());
```

## 相关文章

- [第 09 篇：无状态流处理是玩具](../../Flink/02-State与容错篇/09-无状态流处理是玩具.md)
- [第 10 篇：Keyed State 四剑客](../../Flink/02-State与容错篇/10-Keyed%20State四剑客.md)
- [第 12 篇：状态后端三巨头](../../Flink/02-State与容错篇/12-状态后端三巨头.md)

## 参考资料

- [Flink Operator State 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
- [Broadcast State Pattern](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/broadcast_state/)
- [CheckpointedFunction 接口](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/checkpoint/CheckpointedFunction.html)
