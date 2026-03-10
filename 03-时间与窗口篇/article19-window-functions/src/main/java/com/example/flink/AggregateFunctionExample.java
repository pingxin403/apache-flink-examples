package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * AggregateFunction 示例
 * 
 * 特点:
 * - 增量聚合,内存占用小
 * - 输入、中间、输出类型可以不同
 * - 性能优秀
 * 
 * 适用场景:
 * - 需要计算平均值、方差等复杂指标
 * - 需要类型转换
 * - 对性能要求高
 */
public class AggregateFunctionExample {
    
    /**
     * 累加器:保存中间聚合结果
     */
    public static class OrderAccumulator {
        public BigDecimal totalAmount = BigDecimal.ZERO;
        public int count = 0;
    }

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

        // 使用 AggregateFunction 进行窗口聚合
        DataStream<OrderStats> result = orders
                .keyBy(order -> "all")  // 全局聚合
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new AggregateFunction<Order, OrderAccumulator, OrderStats>() {
                    @Override
                    public OrderAccumulator createAccumulator() {
                        // 创建累加器
                        return new OrderAccumulator();
                    }

                    @Override
                    public OrderAccumulator add(Order order, OrderAccumulator accumulator) {
                        // 将新元素添加到累加器
                        accumulator.totalAmount = accumulator.totalAmount.add(order.getAmount());
                        accumulator.count++;
                        return accumulator;
                    }

                    @Override
                    public OrderStats getResult(OrderAccumulator accumulator) {
                        // 从累加器获取最终结果
                        BigDecimal avgAmount = accumulator.count > 0
                                ? accumulator.totalAmount.divide(
                                        BigDecimal.valueOf(accumulator.count), 
                                        2, 
                                        RoundingMode.HALF_UP)
                                : BigDecimal.ZERO;

                        return new OrderStats(
                                "all",
                                0,  // 窗口开始时间(无法获取)
                                0,  // 窗口结束时间(无法获取)
                                accumulator.totalAmount,
                                accumulator.count,
                                avgAmount
                        );
                    }

                    @Override
                    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
                        // 合并两个累加器(用于会话窗口)
                        a.totalAmount = a.totalAmount.add(b.totalAmount);
                        a.count += b.count;
                        return a;
                    }
                });

        result.print("AggregateFunction Result");

        env.execute("AggregateFunction Example");
    }
}
