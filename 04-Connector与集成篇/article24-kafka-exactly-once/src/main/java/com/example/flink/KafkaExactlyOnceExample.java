package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

/**
 * Kafka + Flink Exactly-Once 完整示例
 * 
 * 功能:
 * 1. 从 Kafka 读取订单数据
 * 2. 按用户ID分组,5分钟滚动窗口统计
 * 3. 将统计结果写入 Kafka
 * 4. 保证端到端 Exactly-Once 语义
 */
public class KafkaExactlyOnceExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Checkpoint(关键:启用 Exactly-Once)
        // 每 60 秒做一次 Checkpoint
        env.enableCheckpointing(60000);
        
        // 设置 Checkpoint 模式为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 设置 Checkpoint 超时时间(10 分钟)
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        
        // 设置两次 Checkpoint 之间的最小间隔(30 秒)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        
        // 设置最多同时进行的 Checkpoint 数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // 作业取消时保留 Checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            org.apache.flink.streaming.api.environment.CheckpointConfig
                .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 3. 配置 Kafka Source
        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "localhost:9092");
        sourceProps.setProperty("group.id", "flink-exactly-once-demo");
        
        // 关键:禁用自动提交 offset
        sourceProps.setProperty("enable.auto.commit", "false");
        
        // 启用动态分区发现(每 30 秒检查一次)
        sourceProps.setProperty("flink.partition-discovery.interval-millis", "30000");

        // 创建 Kafka Consumer
        FlinkKafkaConsumer<Order> consumer = new FlinkKafkaConsumer<>(
            "orders",                           // Topic 名称
            new OrderDeserializationSchema(),   // 反序列化 Schema
            sourceProps                         // Kafka 配置
        );
        
        // 关键:启用 Checkpoint 时提交 offset
        consumer.setCommitOffsetsOnCheckpoints(true);
        
        // 设置起始位置(仅首次启动时生效)
        consumer.setStartFromLatest();

        // 4. 配置 Kafka Sink
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "localhost:9092");
        
        // 关键:设置事务超时时间(15 分钟,必须小于 Broker 的 transaction.max.timeout.ms)
        sinkProps.setProperty("transaction.timeout.ms", "900000");
        
        // 设置事务 ID 前缀
        sinkProps.setProperty("transactional.id.prefix", "flink-kafka-exactly-once-");

        FlinkKafkaProducer<OrderStats> producer = new FlinkKafkaProducer<>(
            "order-stats",                          // Topic 名称
            new OrderStatsSerializationSchema(),    // 序列化 Schema
            sinkProps,                              // Kafka 配置
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // 关键:启用 Exactly-Once 语义
        );

        // 5. 数据处理逻辑
        DataStream<Order> orders = env.addSource(consumer)
            .name("Kafka Source")
            // 设置 Watermark 策略(允许 5 秒延迟)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );

        // 按用户ID分组,5分钟滚动窗口统计
        DataStream<OrderStats> stats = orders
            .keyBy(Order::getUserId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new OrderAggregateFunction(), new OrderWindowFunction())
            .name("Order Aggregation");

        // 6. 写入 Kafka
        stats.addSink(producer)
            .name("Kafka Sink");

        // 7. 执行作业
        env.execute("Kafka Exactly-Once Example");
    }
}
