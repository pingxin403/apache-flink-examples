# Article 24: Kafka + Flink Exactly-Once 示例

本示例演示如何使用 Flink 实现 Kafka 的端到端 Exactly-Once 语义。

## 功能说明

- 从 Kafka Topic `orders` 读取订单数据
- 按用户ID分组,5分钟滚动窗口统计订单数量和金额
- 将统计结果写入 Kafka Topic `order-stats`
- 保证端到端 Exactly-Once 语义(消息不丢失、不重复)

## 核心特性

### 1. Source 端 Exactly-Once
- 禁用 Kafka 自动提交 offset
- 由 Flink 管理 offset,在 Checkpoint 完成后提交
- 支持动态分区发现

### 2. Sink 端 Exactly-Once
- 使用 Kafka 事务 API
- 两阶段提交协议
- 事务超时配置

### 3. Checkpoint 配置
- 60 秒 Checkpoint 间隔
- EXACTLY_ONCE 模式
- 10 分钟 Checkpoint 超时

## 前置条件

1. **Kafka 环境**
   - Kafka 版本 >= 0.11(支持事务)
   - 配置 `transaction.max.timeout.ms >= 900000`

2. **Flink 环境**
   - Flink 版本 >= 1.17
   - 启用 Checkpoint

## 快速开始

### 1. 启动 Kafka

```bash
# 启动 Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# 启动 Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

### 2. 创建 Kafka Topics

```bash
# 创建输入 Topic
bin/kafka-topics.sh --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# 创建输出 Topic
bin/kafka-topics.sh --create \
  --topic order-stats \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. 编译项目

```bash
mvn clean package
```

### 4. 运行 Flink 作业

```bash
flink run -c com.example.flink.KafkaExactlyOnceExample \
  target/article24-kafka-exactly-once-1.0-SNAPSHOT.jar
```

### 5. 发送测试数据

```bash
# 发送订单数据到 Kafka
for i in {1..1000}; do
  echo "{\"orderId\":\"order-$i\",\"userId\":\"user-$((i%10))\",\"productId\":\"product-$((RANDOM%100))\",\"amount\":$((RANDOM%1000)),\"timestamp\":$(date +%s)000}" | \
  bin/kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092
done
```

### 6. 查看结果

```bash
# 消费输出 Topic
bin/kafka-console-consumer.sh \
  --topic order-stats \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true
```

## 验证 Exactly-Once

### 测试步骤

1. **启动作业并发送数据**
   ```bash
   # 发送 1000 条订单数据
   ./send-test-data.sh
   ```

2. **记录初始结果**
   ```bash
   # 记录输出 Topic 的消息数量
   bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
     --broker-list localhost:9092 \
     --topic order-stats
   ```

3. **模拟作业失败**
   ```bash
   # 在 Flink Web UI 中取消作业
   # 或者直接 kill TaskManager 进程
   ```

4. **从 Checkpoint 恢复**
   ```bash
   # 从最近的 Checkpoint 恢复作业
   flink run -s hdfs:///flink/checkpoints/xxx \
     -c com.example.flink.KafkaExactlyOnceExample \
     target/article24-kafka-exactly-once-1.0-SNAPSHOT.jar
   ```

5. **验证结果**
   ```bash
   # 再次检查输出 Topic 的消息数量
   # 应该与步骤2的结果一致,没有重复消息
   ```

## 配置说明

### Kafka Source 配置

```java
Properties sourceProps = new Properties();
sourceProps.setProperty("bootstrap.servers", "localhost:9092");
sourceProps.setProperty("group.id", "flink-exactly-once-demo");
sourceProps.setProperty("enable.auto.commit", "false");  // 关键:禁用自动提交
sourceProps.setProperty("flink.partition-discovery.interval-millis", "30000");
```

### Kafka Sink 配置

```java
Properties sinkProps = new Properties();
sinkProps.setProperty("bootstrap.servers", "localhost:9092");
sinkProps.setProperty("transaction.timeout.ms", "900000");  // 15 分钟
```

### Checkpoint 配置

```java
env.enableCheckpointing(60000);  // 60 秒
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(600000);  // 10 分钟
```

## 常见问题

### 1. 事务超时错误

**错误信息**:
```
org.apache.kafka.common.errors.InvalidTxnTimeoutException: 
The transaction timeout is larger than the maximum value allowed by the broker
```

**解决方案**:
- 检查 Kafka Broker 的 `transaction.max.timeout.ms` 配置
- 确保 `transaction.timeout.ms <= transaction.max.timeout.ms`

### 2. Checkpoint 超时

**错误信息**:
```
Checkpoint expired before completing
```

**解决方案**:
- 增加 Checkpoint 超时时间
- 减少 Checkpoint 间隔
- 优化状态大小

### 3. 数据重复

**可能原因**:
- 未启用 Checkpoint
- `enable.auto.commit=true`
- Semantic 设置为 `AT_LEAST_ONCE`

**解决方案**:
- 启用 Checkpoint
- 设置 `enable.auto.commit=false`
- 设置 `Semantic.EXACTLY_ONCE`

## 性能优化

### 1. Checkpoint 间隔
- 常规场景:60 秒
- 高吞吐场景:3-5 分钟
- 低延迟场景:10-30 秒

### 2. 并行度配置
- Source 并行度 = Kafka 分区数
- Sink 并行度可以大于分区数

### 3. 批量大小
```java
// 配置批量发送大小
sinkProps.setProperty("batch.size", "16384");
sinkProps.setProperty("linger.ms", "10");
```

## 监控指标

### 关键指标
- `flink_jobmanager_job_numberOfCompletedCheckpoints`: Checkpoint 成功次数
- `flink_jobmanager_job_numberOfFailedCheckpoints`: Checkpoint 失败次数
- `flink_taskmanager_job_task_operator_KafkaConsumer_records_lag_max`: Kafka Lag
- `flink_taskmanager_job_task_operator_KafkaProducer_transaction_commit_time_ms`: 事务提交延迟

## 参考资料

- [Flink Kafka Connector 文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [Kafka 事务机制](https://kafka.apache.org/documentation/#semantics)
- [Flink Checkpoint 机制](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)

## 相关文章

- [第 13 篇:Checkpoint 背后的 Chandy-Lamport 算法](../../../Flink/02-State与容错篇/13-Checkpoint背后的Chandy-Lamport算法.md)
- [第 24 篇:Kafka + Flink Exactly-Once](../../../Flink/04-Connector与集成篇/24-Kafka+Flink%20Exactly-Once.md)
