package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 端到端 Exactly-Once 示例：Kafka → Flink → Kafka
 *
 * 本示例演示如何构建一个端到端 Exactly-Once 的数据管道：
 * 1. 从 Kafka input-topic 读取消息（Source 端：offset 由 Checkpoint 管理）
 * 2. 在 Flink 中进行简单的数据转换处理
 * 3. 将结果写入 Kafka output-topic（Sink 端：使用 Kafka 事务 + 2PC 协议）
 *
 * 核心要点：
 * - Flink Checkpoint 保证内部状态一致性（Chandy-Lamport 算法）
 * - KafkaSource 的 offset 保存在 Checkpoint 状态中，支持精确重放
 * - KafkaSink 使用 EXACTLY_ONCE 投递保证，通过 Kafka 事务实现 2PC
 * - 下游消费者必须设置 isolation.level=read_committed 才能真正实现 E2E Exactly-Once
 *
 * 运行前提：
 * - 本地或远程 Kafka 集群已启动
 * - 已创建 input-topic 和 output-topic
 * - Kafka Broker 的 transaction.max.timeout.ms >= 本示例中的 transaction.timeout.ms
 */
public class E2EExactlyOnceExample {

    // Kafka 连接地址（请根据实际环境修改）
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    // 输入 Topic
    private static final String INPUT_TOPIC = "input-topic";
    // 输出 Topic
    private static final String OUTPUT_TOPIC = "output-topic";
    // 消费者组 ID
    private static final String GROUP_ID = "flink-e2e-exactly-once-group";
    // 事务 ID 前缀（每个作业应唯一，避免事务冲突）
    private static final String TXN_ID_PREFIX = "flink-e2e-txn-";

    public static void main(String[] args) throws Exception {
        // ========== 1. 创建执行环境并配置 Checkpoint ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 启用 Checkpoint，间隔 60 秒
        env.enableCheckpointing(60_000L);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        // 设置 Exactly-Once 语义（默认值，这里显式声明以强调）
        cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint 超时时间：120 秒
        // 注意：transaction.timeout.ms 必须 > checkpointInterval + checkpointTimeout
        cpConfig.setCheckpointTimeout(120_000L);
        // 两次 Checkpoint 之间的最小间隔：30 秒（避免 Checkpoint 过于频繁）
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        // 允许同时进行的 Checkpoint 数量
        cpConfig.setMaxConcurrentCheckpoints(1);
        // 作业取消时保留 Checkpoint（便于恢复）
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ========== 2. 构建 Kafka Source ==========
        // KafkaSource 的 offset 由 Flink Checkpoint 管理，而非提交到 Kafka __consumer_offsets
        // 故障恢复时从 Checkpoint 中记录的 offset 精确重放
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(GROUP_ID)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 首次启动从已提交的 offset 开始消费，如果没有则从最早位置开始
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        // ========== 3. 构建 Kafka Sink（启用事务写入） ==========
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(OUTPUT_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                // 关键配置：设置投递保证为 EXACTLY_ONCE
                // 这会让 KafkaSink 使用 Kafka 事务，配合 Checkpoint 实现 2PC
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 事务 ID 前缀：每个作业必须唯一，否则会导致事务冲突
                .setTransactionalIdPrefix(TXN_ID_PREFIX)
                // Kafka Producer 事务超时时间（毫秒）
                // 必须满足：transaction.timeout.ms > checkpointInterval + checkpointTimeout
                // 本例：600000 > 60000 + 120000 = 180000 ✅
                // 同时 Broker 的 transaction.max.timeout.ms 必须 >= 此值
                .setProperty("transaction.timeout.ms", "600000")
                .build();

        // ========== 4. 构建数据处理管道 ==========
        DataStream<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 简单的数据转换：为每条消息添加处理标记
        // 实际生产中这里可以是复杂的业务逻辑（聚合、Join、窗口计算等）
        DataStream<String> processedStream = sourceStream
                .map(value -> "[processed] " + value)
                .name("Data Processor");

        // 将处理结果写入 Kafka（通过事务保证 Exactly-Once）
        processedStream.sinkTo(kafkaSink).name("Kafka Sink (E2E Exactly-Once)");

        // ========== 5. 执行作业 ==========
        env.execute("Kafka-to-Kafka E2E Exactly-Once Pipeline");
    }
}
