package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * ProcessWindowFunction 示例
 * 
 * 特点:
 * - 全量聚合,可以访问窗口内所有元素
 * - 可以访问窗口元数据(窗口时间、Key等)
 * - 内存占用大,性能较差
 * 
 * 适用场景:
 * - 需要访问窗口内所有元素(如排序、去重)
 * - 需要访问窗口元数据
 * - 数据量较小的场景
 */
public class ProcessWindowFunctionExample {
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

        // 使用 ProcessWindowFunction 进行窗口聚合
        DataStream<OrderStats> result = orders
                .keyBy(order -> "all")  // 全局聚合
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ProcessWindowFunction<Order, OrderStats, String, TimeWindow>() {
                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<Order> elements,
                            Collector<OrderStats> out
                    ) {
                        // 可以访问窗口元数据
                        TimeWindow window = context.window();
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();

                        // 可以遍历窗口内的所有元素
                        BigDecimal totalAmount = BigDecimal.ZERO;
                        int count = 0;

                        for (Order order : elements) {
                            totalAmount = totalAmount.add(order.getAmount());
                            count++;
                        }

                        // 计算平均值
                        BigDecimal avgAmount = count > 0
                                ? totalAmount.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP)
                                : BigDecimal.ZERO;

                        // 输出结果
                        out.collect(new OrderStats(
                                key,
                                windowStart,
                                windowEnd,
                                totalAmount,
                                count,
                                avgAmount
                        ));
                    }
                });

        result.print("ProcessWindowFunction Result");

        env.execute("ProcessWindowFunction Example");
    }
}
