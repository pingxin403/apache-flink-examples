package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 三种投递保证对比示例
 *
 * 本示例展示如何分别构建 AT_LEAST_ONCE 和 EXACTLY_ONCE 两种投递保证的 KafkaSink，
 * 帮助理解不同投递语义在配置上的差异。
 *
 * 对比要点：
 * - AT_LEAST_ONCE：不使用 Kafka 事务，吞吐更高，延迟更低，但可能产生重复数据
 * - EXACTLY_ONCE：使用 Kafka 事务 + 2PC，数据精确一致，但有额外的延迟和吞吐开销
 *
 * 选型建议：
 * - 日志分析、监控指标等容忍少量重复的场景 → AT_LEAST_ONCE
 * - 金融交易、计费对账等要求精确一致的场景 → EXACTLY_ONCE
 */
public class DeliveryGuaranteeCompareExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "input-topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 两种模式都需要启用 Checkpoint
        env.enableCheckpointing(60_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 构建 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("delivery-guarantee-compare-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> sourceStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // ========== 方案一：AT_LEAST_ONCE（至少一次） ==========
        // 不使用 Kafka 事务，数据写入后立即对消费者可见
        // 优点：延迟低、吞吐高
        // 缺点：Checkpoint 恢复后可能产生重复数据
        KafkaSink<String> atLeastOnceSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-at-least-once")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                // AT_LEAST_ONCE 不需要事务相关配置
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // ========== 方案二：EXACTLY_ONCE（恰好一次） ==========
        // 使用 Kafka 事务，数据在 Checkpoint 完成后才对消费者可见
        // 优点：数据精确一致，不丢不重
        // 缺点：延迟增加（≈ Checkpoint 间隔），吞吐下降约 10%-20%
        KafkaSink<String> exactlyOnceSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-exactly-once")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // EXACTLY_ONCE 必须配置事务 ID 前缀
                .setTransactionalIdPrefix("compare-e2e-txn-")
                // 事务超时必须 > Checkpoint 间隔 + Checkpoint 超时
                .setProperty("transaction.timeout.ms", "600000")
                .build();

        // 将同一份数据分别写入两个 Sink，方便对比
        DataStream<String> processed = sourceStream
                .map(value -> "[compare] " + value)
                .name("Data Processor");

        // 这里只演示 Exactly-Once Sink 的使用
        // 如需同时写入两个 Sink，可以使用 side output 或分流
        processed.sinkTo(exactlyOnceSink).name("Exactly-Once Sink");

        env.execute("Delivery Guarantee Compare Example");
    }
}
