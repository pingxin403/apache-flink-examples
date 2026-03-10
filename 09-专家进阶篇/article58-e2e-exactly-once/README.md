# 端到端 Exactly-Once：Kafka → Flink → Kafka 全链路保障

本项目是「Apache Flink 从入门到专家」系列第 58 篇文章的配套代码示例。

## 项目说明

演示如何构建 Kafka → Flink → Kafka 端到端 Exactly-Once 数据管道，包含以下示例：

| 示例类 | 说明 |
|:---|:---|
| `E2EExactlyOnceExample` | 完整的端到端 Exactly-Once 管道，展示 KafkaSource + KafkaSink 的事务配置 |
| `DeliveryGuaranteeCompareExample` | AT_LEAST_ONCE 与 EXACTLY_ONCE 两种投递保证的配置对比 |

## 核心知识点

1. **Flink Checkpoint 配置**：启用 Exactly-Once 语义，合理设置间隔和超时
2. **KafkaSource offset 管理**：offset 由 Checkpoint 管理，支持精确重放
3. **KafkaSink 事务写入**：通过 `DeliveryGuarantee.EXACTLY_ONCE` 启用 Kafka 事务
4. **2PC 协议**：Checkpoint Barrier 触发 Pre-Commit，Checkpoint 成功后 Commit
5. **关键参数**：`transaction.timeout.ms`、`transactional.id.prefix`、`isolation.level`

## 运行前提

1. 本地或远程 Kafka 集群已启动（推荐 Kafka 3.x+）
2. 创建所需的 Topic：

```bash
# 创建输入 Topic
kafka-topics.sh --create --topic input-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# 创建输出 Topic
kafka-topics.sh --create --topic output-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
```

3. 确保 Kafka Broker 配置中 `transaction.max.timeout.ms` >= 600000（10 分钟）

## 编译与运行

```bash
# 编译
mvn clean package -DskipTests

# 向输入 Topic 发送测试数据
kafka-console-producer.sh --topic input-topic --bootstrap-server localhost:9092
> hello
> world
> exactly-once-test

# 运行 Flink 作业（本地模式）
java -cp target/article58-e2e-exactly-once-1.0-SNAPSHOT.jar com.example.flink.E2EExactlyOnceExample

# 消费输出 Topic（注意设置 isolation.level=read_committed）
kafka-console-consumer.sh --topic output-topic --bootstrap-server localhost:9092 \
  --consumer-property isolation.level=read_committed --from-beginning
```

## 关键配置清单

| 配置项 | 位置 | 推荐值 | 说明 |
|:---|:---|:---|:---|
| `checkpointInterval` | Flink | 60000 ms | Checkpoint 触发间隔 |
| `checkpointTimeout` | Flink | 120000 ms | Checkpoint 超时时间 |
| `delivery.guarantee` | KafkaSink | EXACTLY_ONCE | 启用事务写入 |
| `transaction.timeout.ms` | KafkaSink | 600000 ms | 必须 > interval + timeout |
| `transactional.id.prefix` | KafkaSink | 作业唯一标识 | 避免事务 ID 冲突 |
| `transaction.max.timeout.ms` | Kafka Broker | 900000 ms | 必须 ≥ Producer 的事务超时 |
| `isolation.level` | 下游 Consumer | read_committed | 只读已提交的事务数据 |

## 常见问题

**Q: 为什么下游消费者读到了重复数据？**
A: 检查下游消费者是否设置了 `isolation.level=read_committed`，默认值 `read_uncommitted` 会读到未提交的事务数据。

**Q: 作业启动报 ProducerFencedException？**
A: 可能是事务 ID 冲突。确保 `transactional.id.prefix` 在不同作业间唯一，或等待旧事务超时后再启动。

**Q: Checkpoint 总是超时？**
A: 检查 `transaction.timeout.ms` 是否大于 Checkpoint 间隔 + 超时时间，同时检查 Broker 的 `transaction.max.timeout.ms` 是否足够大。

## 参考链接

- [文章原文](https://github.com/pingxin403/apache-flink-examples)
- [Flink Kafka Connector 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [Flink Checkpointing 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)
