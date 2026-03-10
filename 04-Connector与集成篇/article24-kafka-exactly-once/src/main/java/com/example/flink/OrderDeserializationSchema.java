package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 订单反序列化 Schema
 * 将 Kafka 中的 JSON 字符串反序列化为 Order 对象
 */
public class OrderDeserializationSchema implements DeserializationSchema<Order> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Order deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        
        try {
            return objectMapper.readValue(message, Order.class);
        } catch (Exception e) {
            // 记录错误日志,返回 null(Flink 会跳过这条消息)
            System.err.println("Failed to deserialize message: " + new String(message));
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Order nextElement) {
        // 永不结束(流式处理)
        return false;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}
