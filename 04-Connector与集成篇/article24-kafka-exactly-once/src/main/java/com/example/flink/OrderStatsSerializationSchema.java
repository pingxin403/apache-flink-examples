package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * 订单统计结果序列化 Schema
 * 将 OrderStats 对象序列化为 JSON 字符串写入 Kafka
 */
public class OrderStatsSerializationSchema implements KafkaSerializationSchema<OrderStats> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public OrderStatsSerializationSchema() {
        this.topic = "order-stats";
    }

    public OrderStatsSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OrderStats element, @Nullable Long timestamp) {
        try {
            // 将 OrderStats 序列化为 JSON
            String json = objectMapper.writeValueAsString(element);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            
            // 使用 userId 作为 Kafka 消息的 key,保证同一用户的消息发送到同一分区
            byte[] key = element.getUserId().getBytes(StandardCharsets.UTF_8);
            
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            System.err.println("Failed to serialize OrderStats: " + element);
            e.printStackTrace();
            return null;
        }
    }
}
