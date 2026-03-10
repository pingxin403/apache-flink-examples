package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * ReduceFunction 示例
 * 
 * 特点:
 * - 增量聚合,内存占用最小
 * - 输入输出类型必须相同
 * - 性能最优
 * 
 * 适用场景:
 * - 简单的累加、求和操作
 * - 输入输出类型相同
 * - 对性能要求极高
 */
public class ReduceFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建订单数据流
        DataStream<Order> orders = env
                .addSource(new OrderSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
                );

        // 使用 ReduceFunction 进行窗口聚合
        DataStream<Order> result = orders
                .keyBy(order -> "all")  // 全局聚合
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .reduce(new ReduceFunction<Order>() {
                    @Override
                    public Order reduce(Order order1, Order order2) {
                        // 将两个订单合并成一个
                        // 注意:输入和输出类型必须相同
                        return new Order(
                                "MERGED",
                                "ALL",
                                "ALL",
                                order1.getAmount().add(order2.getAmount()),  // 金额累加
                                order1.getTimestamp()
                        );
                    }
                });

        result.print("ReduceFunction Result");

        env.execute("ReduceFunction Example");
    }
}
