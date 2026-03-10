package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 迟到数据处理示例：Allowed Lateness + Side Output
 * 
 * 本示例演示如何使用 Allowed Lateness 和 Side Output 机制处理迟到数据：
 * 1. 使用 Watermark 处理乱序数据（10秒延迟）
 * 2. 使用 Allowed Lateness 延长窗口生命周期（30秒）
 * 3. 使用 Side Output 捕获超时迟到数据
 */
public class LateDataHandlingExample {

    // 定义侧输出标签，用于捕获迟到数据
    private static final OutputTag<Order> LATE_DATA_TAG = new OutputTag<Order>("late-data"){};

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 设置并行度为1，方便观察结果

        // 2. 创建订单数据源（模拟包含迟到数据的场景）
        DataStream<Order> orders = env
                .addSource(new OrderSourceWithLateData())
                .name("Order Source");

        // 3. 分配时间戳和 Watermark
        // Watermark 延迟10秒，意味着允许最多10秒的乱序
        DataStream<Order> ordersWithWatermark = orders
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
                )
                .name("Assign Watermarks");

        // 4. 窗口聚合：配置 Allowed Lateness 和 Side Output
        SingleOutputStreamOperator<OrderStats> result = ordersWithWatermark
                .keyBy(order -> "all")  // 全局聚合
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .allowedLateness(Time.seconds(30))  // 允许30秒延迟
                .sideOutputLateData(LATE_DATA_TAG)  // 捕获超时迟到数据
                .aggregate(new OrderAggregateFunction())
                .name("Window Aggregate");

        // 5. 主流：打印正常结果
        result.map(stats -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());
            String windowStart = formatter.format(Instant.ofEpochMilli(stats.getWindowStart()));
            String windowEnd = formatter.format(Instant.ofEpochMilli(stats.getWindowEnd()));
            
            return String.format("[主流] 窗口 [%s, %s): 订单总额 = %.2f 元, 订单数量 = %d, 版本 = %d",
                    windowStart, windowEnd, stats.getTotalAmount(), stats.getOrderCount(), stats.getVersion());
        }).print();

        // 6. 侧输出流：打印迟到数据告警
        DataStream<Order> lateData = result.getSideOutput(LATE_DATA_TAG);
        lateData.map(order -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());
            String eventTime = formatter.format(Instant.ofEpochMilli(order.getTimestamp()));
            long latency = System.currentTimeMillis() - order.getTimestamp();
            
            return String.format("[侧输出] 迟到订单: orderId=%s, eventTime=%s, latency=%dms, amount=%.2f",
                    order.getOrderId(), eventTime, latency, order.getAmount());
        }).print();

        // 7. 执行作业
        env.execute("Late Data Handling Example");
    }

    /**
     * 订单聚合函数
     * 计算窗口内的订单总额和订单数量
     */
    private static class OrderAggregateFunction 
            implements AggregateFunction<Order, OrderAccumulator, OrderStats> {

        @Override
        public OrderAccumulator createAccumulator() {
            return new OrderAccumulator();
        }

        @Override
        public OrderAccumulator add(Order order, OrderAccumulator accumulator) {
            accumulator.totalAmount = accumulator.totalAmount.add(order.getAmount());
            accumulator.orderCount++;
            return accumulator;
        }

        @Override
        public OrderStats getResult(OrderAccumulator accumulator) {
            return new OrderStats(
                    accumulator.windowStart,
                    accumulator.windowEnd,
                    accumulator.totalAmount,
                    accumulator.orderCount
            );
        }

        @Override
        public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
            a.totalAmount = a.totalAmount.add(b.totalAmount);
            a.orderCount += b.orderCount;
            return a;
        }
    }

    /**
     * 订单累加器
     */
    private static class OrderAccumulator {
        BigDecimal totalAmount = BigDecimal.ZERO;
        long orderCount = 0;
        long windowStart = 0;
        long windowEnd = 0;
    }
}
