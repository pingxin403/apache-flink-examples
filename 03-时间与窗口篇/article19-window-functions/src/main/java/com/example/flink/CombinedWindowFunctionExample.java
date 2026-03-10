package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * 组合使用 AggregateFunction 和 ProcessWindowFunction
 * 
 * 特点:
 * - 兼顾性能和灵活性
 * - AggregateFunction 负责增量聚合
 * - ProcessWindowFunction 负责添加窗口元数据
 * 
 * 适用场景:
 * - 需要增量聚合的性能
 * - 需要访问窗口元数据
 * - 大多数生产场景的最佳选择
 */
public class CombinedWindowFunctionExample {
    
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

        // 组合使用 AggregateFunction 和 ProcessWindowFunction
        DataStream<OrderStats> result = orders
                .keyBy(order -> "all")  // 全局聚合
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(
                        // 第一步:AggregateFunction 进行增量聚合
                        new AggregateFunction<Order, OrderAccumulator, OrderAccumulator>() {
                            @Override
                            public OrderAccumulator createAccumulator() {
                                return new OrderAccumulator();
                            }

                            @Override
                            public OrderAccumulator add(Order order, OrderAccumulator accumulator) {
                                accumulator.totalAmount = accumulator.totalAmount.add(order.getAmount());
                                accumulator.count++;
                                return accumulator;
                            }

                            @Override
                            public OrderAccumulator getResult(OrderAccumulator accumulator) {
                                // 返回累加器本身,传递给 ProcessWindowFunction
                                return accumulator;
                            }

                            @Override
                            public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
                                a.totalAmount = a.totalAmount.add(b.totalAmount);
                                a.count += b.count;
                                return a;
                            }
                        },
                        // 第二步:ProcessWindowFunction 添加窗口元数据
                        new ProcessWindowFunction<OrderAccumulator, OrderStats, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<OrderAccumulator> elements,
                                    Collector<OrderStats> out
                            ) {
                                // 获取累加器(只有一个元素)
                                OrderAccumulator accumulator = elements.iterator().next();

                                // 获取窗口元数据
                                TimeWindow window = context.window();
                                long windowStart = window.getStart();
                                long windowEnd = window.getEnd();

                                // 计算平均值
                                BigDecimal avgAmount = accumulator.count > 0
                                        ? accumulator.totalAmount.divide(
                                                BigDecimal.valueOf(accumulator.count), 
                                                2, 
                                                RoundingMode.HALF_UP)
                                        : BigDecimal.ZERO;

                                // 输出结果
                                out.collect(new OrderStats(
                                        key,
                                        windowStart,
                                        windowEnd,
                                        accumulator.totalAmount,
                                        accumulator.count,
                                        avgAmount
                                ));
                            }
                        }
                );

        result.print("Combined Result");

        env.execute("Combined Window Function Example");
    }
}
