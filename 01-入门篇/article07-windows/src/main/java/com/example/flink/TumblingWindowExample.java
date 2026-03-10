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
 * Tumbling Window（滚动窗口）示例
 * 
 * 功能：统计每分钟的订单量
 * 
 * 窗口特点：
 * - 窗口大小固定（1分钟）
 * - 窗口之间不重叠
 * - 每个数据只属于一个窗口
 */
public class TumblingWindowExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 设置并行度为1，方便观察结果
        
        // 2. 创建订单数据流
        DataStream<Order> orders = env
            .addSource(new OrderSource())
            .name("Order Source");
        
        // 3. 分配时间戳和 Watermark
        DataStream<Order> ordersWithTimestamp = orders
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))  // 允许5秒的乱序
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );
        
        // 4. 使用 Tumbling Window 统计每分钟的订单量
        DataStream<Tuple2<String, Long>> result = ordersWithTimestamp
            .map(order -> Tuple2.of("order_count", 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 10秒的滚动窗口（演示用）
            .sum(1);
        
        // 5. 打印结果
        result.print("Tumbling Window Result");
        
        // 6. 执行作业
        env.execute("Tumbling Window Example");
    }
}
