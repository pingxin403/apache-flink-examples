package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Random;

/**
 * State TTL 综合示例
 * 演示三种不同的 TTL 使用场景
 */
public class StateTtlExample {
    
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 配置 RocksDB 状态后端（支持 RocksDB 压缩清理）
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        
        // 启用 Checkpoint
        env.enableCheckpointing(60000);  // 每 60 秒一次
        
        System.out.println("=== Flink State TTL 示例 ===\n");
        
        // 示例 1：用户会话管理
        runSessionExample(env);
        
        // 示例 2：商品销量统计
        runSalesExample(env);
        
        // 示例 3：设备异常检测
        runAnomalyDetectionExample(env);
        
        // 执行作业
        env.execute("State TTL Example");
    }
    
    /**
     * 示例 1：用户会话管理
     * TTL: 30 分钟，任何活动都重置
     */
    private static void runSessionExample(StreamExecutionEnvironment env) {
        System.out.println("示例 1：用户会话管理（TTL: 30 分钟）");
        
        // 生成模拟用户事件
        DataStream<UserEvent> userEvents = env
            .fromElements(
                new UserEvent("user1", "login", System.currentTimeMillis()),
                new UserEvent("user1", "click", System.currentTimeMillis() + 1000),
                new UserEvent("user2", "login", System.currentTimeMillis() + 2000),
                new UserEvent("user1", "purchase", System.currentTimeMillis() + 3000)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
            );
        
        // 应用会话管理
        DataStream<SessionAlert> sessionAlerts = userEvents
            .keyBy(UserEvent::getUserId)
            .process(new SessionManager());
        
        // 输出结果
        sessionAlerts.print("会话告警");
    }
    
    /**
     * 示例 2：商品销量统计
     * TTL: 7 天，只在写入时重置
     */
    private static void runSalesExample(StreamExecutionEnvironment env) {
        System.out.println("\n示例 2：商品销量统计（TTL: 7 天）");
        
        // 生成模拟订单
        DataStream<Order> orders = env
            .fromElements(
                new Order("order1", "product1", 2, System.currentTimeMillis()),
                new Order("order2", "product1", 3, System.currentTimeMillis() + 1000),
                new Order("order3", "product2", 1, System.currentTimeMillis() + 2000),
                new Order("order4", "product1", 5, System.currentTimeMillis() + 3000)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );
        
        // 应用销量统计
        DataStream<ProductSales> sales = orders
            .keyBy(Order::getProductId)
            .process(new ProductSalesCounter());
        
        // 输出结果
        sales.print("商品销量");
    }
    
    /**
     * 示例 3：设备异常检测
     * TTL: 1 小时，只保留最近的读数
     */
    private static void runAnomalyDetectionExample(StreamExecutionEnvironment env) {
        System.out.println("\n示例 3：设备异常检测（TTL: 1 小时）");
        
        // 生成模拟传感器读数
        Random random = new Random();
        DataStream<SensorReading> readings = env
            .fromElements(
                new SensorReading("device1", 20.0 + random.nextDouble() * 5, System.currentTimeMillis()),
                new SensorReading("device1", 22.0 + random.nextDouble() * 5, System.currentTimeMillis() + 1000),
                new SensorReading("device1", 21.0 + random.nextDouble() * 5, System.currentTimeMillis() + 2000),
                new SensorReading("device1", 50.0, System.currentTimeMillis() + 3000),  // 异常值
                new SensorReading("device2", 30.0 + random.nextDouble() * 5, System.currentTimeMillis() + 4000)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((reading, timestamp) -> reading.getTimestamp())
            );
        
        // 应用异常检测
        DataStream<Alert> alerts = readings
            .keyBy(SensorReading::getDeviceId)
            .process(new DeviceAnomalyDetector());
        
        // 输出结果
        alerts.print("异常告警");
    }
}
