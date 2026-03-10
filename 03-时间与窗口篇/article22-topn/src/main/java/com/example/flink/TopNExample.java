package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 实时 TOP-N 排行榜示例
 * 
 * 功能:
 * 1. 模拟订单数据流
 * 2. 按商品ID分组,统计每分钟的销量
 * 3. 计算销量 TOP 10 的商品
 * 4. 实时输出排行榜
 * 
 * 核心技术:
 * - 两阶段聚合:先分组聚合,再全局排序
 * - AggregateFunction:增量聚合,减少状态占用
 * - KeyedProcessFunction + 定时器:确保数据完整性
 * - 事件时间 + Watermark:处理乱序数据
 */
public class TopNExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        env.setParallelism(1);
        
        // 2. 读取订单流
        DataStream<Order> orderStream = env
            .addSource(new OrderSource())
            .name("Order Source");
        
        // 3. 设置事件时间和 Watermark
        DataStream<Order> ordersWithWatermark = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, ts) -> order.getTimestamp())
            );
        
        // 4. 计算 TOP-N
        // 第一阶段:按商品ID分组,统计每个商品在窗口内的销量
        DataStream<ProductSales> salesStream = ordersWithWatermark
            .keyBy(Order::getProductId)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(
                new SalesAggregateFunction(),
                new WindowResultFunction()
            )
            .name("Aggregate Sales by Product");
        
        // 第二阶段:全局排序,输出 TOP-N
        salesStream
            .keyBy(value -> "global")  // 所有数据路由到同一个并行度
            .process(new TopNProcessFunction(10))
            .name("Calculate TOP-N")
            .print();
        
        // 5. 执行作业
        env.execute("Real-time TOP-N Leaderboard Example");
    }
}
