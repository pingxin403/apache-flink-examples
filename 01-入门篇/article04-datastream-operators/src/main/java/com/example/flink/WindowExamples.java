package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 窗口算子示例
 * 演示滚动窗口（Tumbling Window）和滑动窗口（Sliding Window）的使用
 */
public class WindowExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 示例 1：滚动窗口 - 每 10 秒统计一次
        System.out.println("=== 示例 1：滚动窗口 ===");
        tumblingWindowExample(env);

        // 示例 2：滑动窗口 - 每 5 秒统计最近 10 秒的数据
        System.out.println("\n=== 示例 2：滑动窗口 ===");
        slidingWindowExample(env);

        // 执行作业
        env.execute("Window Examples");
    }

    /**
     * 示例 1：滚动窗口
     * 功能：每 10 秒统计一次每个用户的订单总金额
     */
    private static void tumblingWindowExample(StreamExecutionEnvironment env) {
        // 创建模拟数据
        DataStream<Order> orderStream = env.fromElements(
            new Order("user1", 100.0, 1000L),
            new Order("user1", 200.0, 2000L),
            new Order("user2", 150.0, 3000L),
            new Order("user1", 300.0, 11000L),  // 下一个窗口
            new Order("user2", 250.0, 12000L)   // 下一个窗口
        );

        // 提取时间戳和水位线
        DataStream<Order> withTimestamps = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );

        // 滚动窗口：每 10 秒统计一次
        DataStream<Tuple2<String, Double>> result = withTimestamps
            .map(order -> Tuple2.of(order.getUserId(), order.getAmount()))
            .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sum(1);

        result.print("Tumbling Window");
    }

    /**
     * 示例 2：滑动窗口
     * 功能：每 5 秒统计最近 10 秒的订单总金额
     */
    private static void slidingWindowExample(StreamExecutionEnvironment env) {
        // 创建模拟数据
        DataStream<Order> orderStream = env.fromElements(
            new Order("user1", 100.0, 1000L),
            new Order("user1", 200.0, 6000L),
            new Order("user1", 300.0, 11000L)
        );

        // 提取时间戳和水位线
        DataStream<Order> withTimestamps = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            );

        // 滑动窗口：窗口大小 10 秒，滑动步长 5 秒
        DataStream<Tuple2<String, Double>> result = withTimestamps
            .map(order -> Tuple2.of(order.getUserId(), order.getAmount()))
            .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
            .keyBy(tuple -> tuple.f0)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .sum(1);

        result.print("Sliding Window");
    }
}
