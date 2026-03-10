package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * DataStream API Interval Join 示例
 *
 * 演示内容：
 * 1. 使用 DataStream API 实现 Interval Join
 * 2. 订单流与支付流基于事件时间的时间范围关联
 * 3. 通过 between() 定义灵活的时间范围（0 ~ 15 分钟）
 *
 * 与 SQL 版本的区别：
 * - DataStream API 提供更细粒度的控制
 * - 可以在 ProcessJoinFunction 中实现复杂的匹配逻辑
 * - 默认只支持 INNER JOIN
 */
public class DataStreamIntervalJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 模拟订单流：(order_id, user_id, amount, event_time_ms)
        DataStream<Tuple4<String, Integer, Double, Long>> orderStream = env
            .fromElements(
                Tuple4.of("O001", 1, 100.0, Instant.parse("2024-01-01T10:00:00Z").toEpochMilli()),
                Tuple4.of("O002", 2, 200.0, Instant.parse("2024-01-01T10:02:00Z").toEpochMilli()),
                Tuple4.of("O003", 3, 300.0, Instant.parse("2024-01-01T10:05:00Z").toEpochMilli())
            )
            .returns(Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple4<String, Integer, Double, Long>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.f3)
            );

        // 2. 模拟支付流：(pay_id, order_id, pay_amount, pay_channel, event_time_ms)
        DataStream<Tuple5<String, String, Double, String, Long>> paymentStream = env
            .fromElements(
                // O001 的支付，在订单后 3 分钟（在 15 分钟范围内 ✅）
                Tuple5.of("P001", "O001", 100.0, "ALIPAY",
                    Instant.parse("2024-01-01T10:03:00Z").toEpochMilli()),
                // O002 的支付，在订单后 8 分钟（在 15 分钟范围内 ✅）
                Tuple5.of("P002", "O002", 200.0, "WECHAT",
                    Instant.parse("2024-01-01T10:10:00Z").toEpochMilli()),
                // O003 的支付，在订单后 20 分钟（超出 15 分钟范围 ❌）
                Tuple5.of("P003", "O003", 300.0, "BANK",
                    Instant.parse("2024-01-01T10:25:00Z").toEpochMilli())
            )
            .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.STRING, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple5<String, String, Double, String, Long>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.f4)
            );

        // 3. Interval Join：订单后 0~15 分钟内的支付
        // keyBy order_id 进行关联，between 定义时间范围
        orderStream
            .keyBy(order -> order.f0)  // 按 order_id 分组
            .intervalJoin(paymentStream.keyBy(payment -> payment.f1))  // 按 order_id 关联
            .between(Time.minutes(0), Time.minutes(15))  // 时间范围：订单时间后 0~15 分钟
            .process(new ProcessJoinFunction<
                    Tuple4<String, Integer, Double, Long>,
                    Tuple5<String, String, Double, String, Long>,
                    String>() {
                @Override
                public void processElement(
                        Tuple4<String, Integer, Double, Long> order,
                        Tuple5<String, String, Double, String, Long> payment,
                        Context ctx,
                        Collector<String> out) {
                    // 匹配成功，拼接订单和支付信息
                    out.collect(String.format(
                        "订单[%s] 用户=%d 金额=%.2f | 支付[%s] 渠道=%s 金额=%.2f",
                        order.f0, order.f1, order.f2,
                        payment.f0, payment.f3, payment.f2
                    ));
                }
            })
            .print("Interval Join 结果");

        // 预期输出：
        // O001 ⋈ P001 ✅（3 分钟内）
        // O002 ⋈ P002 ✅（8 分钟内）
        // O003 ⋈ P003 ❌（20 分钟，超出范围，不输出）

        env.execute("DataStream Interval Join Example");
    }
}
