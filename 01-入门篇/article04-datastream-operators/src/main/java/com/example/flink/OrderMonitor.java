package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 订单实时监控示例
 * 功能：
 * 1. 从数据源读取订单流
 * 2. 过滤出金额大于 1000 的订单
 * 3. 按用户 ID 分组
 * 4. 统计每个用户最近 1 分钟的订单总金额
 * 5. 输出结果到控制台
 */
public class OrderMonitor {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度为 1，方便观察输出结果
        env.setParallelism(1);

        // 2. Source：从集合读取模拟数据
        // 生产环境通常使用 Kafka 作为数据源
        DataStream<Order> orderStream = env.fromElements(
            new Order("user1", 1500.0, System.currentTimeMillis()),
            new Order("user2", 800.0, System.currentTimeMillis() + 1000),
            new Order("user1", 2000.0, System.currentTimeMillis() + 2000),
            new Order("user3", 1200.0, System.currentTimeMillis() + 3000),
            new Order("user1", 500.0, System.currentTimeMillis() + 4000),
            new Order("user2", 1100.0, System.currentTimeMillis() + 5000),
            new Order("user3", 900.0, System.currentTimeMillis() + 6000)
        );

        // 3. Transformation：数据处理流程
        DataStream<Tuple2<String, Double>> result = orderStream
            // 3.1 filter：过滤出金额大于 1000 的订单
            .filter(order -> {
                boolean pass = order.getAmount() > 1000;
                System.out.println("Filter: " + order + " -> " + (pass ? "PASS" : "REJECT"));
                return pass;
            })
            // 3.2 assignTimestampsAndWatermarks：提取时间戳和水位线
            // 允许 5 秒的乱序数据
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            )
            // 3.3 map：提取 (userId, amount) 元组
            .map(order -> {
                Tuple2<String, Double> tuple = Tuple2.of(order.getUserId(), order.getAmount());
                System.out.println("Map: " + order + " -> " + tuple);
                return tuple;
            })
            .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
            // 3.4 keyBy：按用户 ID 分组
            .keyBy(tuple -> tuple.f0)
            // 3.5 window：1 分钟滚动窗口
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            // 3.6 sum：对金额字段求和
            .sum(1);

        // 4. Sink：输出结果到控制台
        result.print("Result");

        // 5. 执行作业
        env.execute("Order Monitor");
    }
}
