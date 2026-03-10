package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Sliding Window（滑动窗口）示例
 * 
 * 功能：统计最近30秒的订单量，每10秒更新一次
 * 
 * 窗口特点：
 * - 窗口大小固定（30秒）
 * - 窗口之间可以重叠
 * - 每个数据可能属于多个窗口
 * - 滑动步长（10秒）< 窗口大小（30秒），所以窗口会重叠
 */
public class SlidingWindowExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. 创建订单数据流
        DataStream<Order> orders = env
            .addSource(new OrderSource())
            .name("Order Source");
        
        // 3. 分配时间戳和 Watermark
        DataStream<Order> ordersWithTimestamp = orders
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );
        
        // 4. 使用 Sliding Window 统计最近30秒的订单量，每10秒更新一次
        DataStream<Tuple2<String, Long>> result = ordersWithTimestamp
            .map(order -> Tuple2.of("order_count", 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(tuple -> tuple.f0)
            .window(SlidingEventTimeWindows.of(
                Time.seconds(30),  // 窗口大小：30秒
                Time.seconds(10)   // 滑动步长：10秒
            ))
            .sum(1);
        
        // 5. 打印结果
        result.print("Sliding Window Result");
        
        // 6. 执行作业
        env.execute("Sliding Window Example");
    }
}
