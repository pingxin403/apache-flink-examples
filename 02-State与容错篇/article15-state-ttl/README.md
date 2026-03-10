# Article 15: State TTL 示例

本项目演示 Flink 状态 TTL（Time-To-Live）机制的使用，包括三个实战场景。

## 项目结构

```
article15-state-ttl/
├── src/main/java/com/example/flink/
│   ├── StateTtlExample.java          # 综合示例主程序
│   ├── SessionManager.java           # 示例 1：用户会话管理
│   ├── ProductSalesCounter.java      # 示例 2：商品销量统计
│   ├── DeviceAnomalyDetector.java    # 示例 3：设备异常检测
│   └── model/
│       ├── UserEvent.java            # 用户事件
│       ├── UserSession.java          # 用户会话
│       ├── SessionAlert.java         # 会话告警
│       ├── Order.java                # 订单
│       ├── ProductSales.java         # 商品销量
│       ├── SensorReading.java        # 传感器读数
│       └── Alert.java                # 告警信息
├── pom.xml
└── README.md
```

## 示例说明

### 示例 1：用户会话管理（SessionManager）

**场景**：记录用户会话，30 分钟无活动自动过期。

**TTL 配置**：
- TTL 时间：30 分钟
- 更新策略：`OnReadAndWrite`（任何活动都重置 TTL）
- 可见性：`NeverReturnExpired`（过期状态不返回）
- 清理策略：增量清理（每次访问清理 5 个过期条目）

**核心代码**：
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.minutes(30))
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupIncrementally(5, true)
    .build();
```

**适用场景**：
- 用户会话管理
- 在线用户统计
- 活跃度监控

### 示例 2：商品销量统计（ProductSalesCounter）

**场景**：统计商品最近 7 天的销量，超过 7 天的数据自动清理。

**TTL 配置**：
- TTL 时间：7 天
- 更新策略：`OnCreateAndWrite`（只在写入时重置 TTL）
- 可见性：`NeverReturnExpired`
- 清理策略：RocksDB 压缩清理（后台清理，不影响前台性能）

**核心代码**：
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.days(7))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(1000)
    .build();
```

**适用场景**：
- 时间窗口统计
- 历史数据分析
- 趋势监控

### 示例 3：设备异常检测（DeviceAnomalyDetector）

**场景**：记录设备最近 1 小时的读数，检测异常值。

**TTL 配置**：
- TTL 时间：1 小时
- 更新策略：`OnCreateAndWrite`
- 可见性：`NeverReturnExpired`
- 清理策略：增量清理

**核心代码**：
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupIncrementally(10, true)
    .build();
```

**适用场景**：
- 实时监控
- 异常检测
- 滑动窗口分析

## 运行环境

- Java 11+
- Apache Flink 1.17.2
- Maven 3.6+

## 编译和运行

### 1. 编译项目

```bash
cd apache-flink-examples/02-State与容错篇/article15-state-ttl
mvn clean package
```

### 2. 运行示例

```bash
# 方式 1：使用 Maven 运行
mvn exec:java -Dexec.mainClass="com.example.flink.StateTtlExample"

# 方式 2：使用 Flink CLI 运行
flink run -c com.example.flink.StateTtlExample target/article15-state-ttl-1.0-SNAPSHOT.jar
```

### 3. 预期输出

```
=== Flink State TTL 示例 ===

示例 1：用户会话管理（TTL: 30 分钟）
会话告警> SessionAlert{userId='user1', message='会话开始', timestamp=...}
会话告警> SessionAlert{userId='user1', message='会话活动: 1 个事件', timestamp=...}
会话告警> SessionAlert{userId='user2', message='会话开始', timestamp=...}
会话告警> SessionAlert{userId='user1', message='会话活动: 2 个事件', timestamp=...}

示例 2：商品销量统计（TTL: 7 天）
商品销量> ProductSales{productId='product1', salesCount=2, period='最近 7 天销量'}
商品销量> ProductSales{productId='product1', salesCount=5, period='最近 7 天销量'}
商品销量> ProductSales{productId='product2', salesCount=1, period='最近 7 天销量'}
商品销量> ProductSales{productId='product1', salesCount=10, period='最近 7 天销量'}

示例 3：设备异常检测（TTL: 1 小时）
异常告警> Alert{deviceId='device1', message='检测到异常读数: 50.0', timestamp=...}
```

## TTL 配置参数说明

### 更新策略（UpdateType）

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| `OnCreateAndWrite` | 创建和写入时更新 TTL | 只关心最后修改时间 |
| `OnReadAndWrite` | 读取和写入时都更新 TTL | 关心最后访问时间 |
| `Disabled` | 创建后不再更新 TTL | 固定过期时间 |

### 可见性（StateVisibility）

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| `NeverReturnExpired` | 过期状态永不返回 | 严格的过期语义 |
| `ReturnExpiredIfNotCleanedUp` | 未清理的过期状态仍返回 | 宽松的过期语义 |

### 清理策略

| 策略 | 说明 | 性能影响 | 适用场景 |
|------|------|---------|---------|
| `cleanupFullSnapshot()` | Savepoint 时清理 | 小 | 低频访问 |
| `cleanupIncrementally(n, runCleanupForEveryRecord)` | 增量清理 | 中 | 高频访问 |
| `cleanupInRocksdbCompactFilter(queryTimeAfterNumEntries)` | RocksDB 压缩时清理 | 小 | RocksDB 后端 |

## 注意事项

1. **TTL 时间选择**：根据业务需求和数据特性选择合适的 TTL 时间
2. **清理策略选择**：根据状态后端和访问频率选择合适的清理策略
3. **状态大小监控**：定期监控状态大小，确保 TTL 生效
4. **Checkpoint 影响**：全量快照清理会增加 Checkpoint 时间
5. **RocksDB 后端**：使用 RocksDB 时推荐使用压缩清理策略

## 相关文章

- 第 10 篇：Keyed State 四剑客
- 第 12 篇：状态后端三巨头选型指南
- 第 13 篇：Checkpoint 背后的 Chandy-Lamport 算法
- 第 14 篇：Savepoint 不是备份！

## 参考资料

- [Flink State TTL 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
- [Managing State Size](https://flink.apache.org/2020/07/07/flink-sql-demo-building-e2e-streaming-application.html)
