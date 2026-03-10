package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * DataStream API 实现宽表构建示例
 *
 * 演示内容：
 * 1. 使用 DataStream API 的 Interval Join 关联订单流与支付流
 * 2. 在 ProcessJoinFunction 中实现自定义的宽表拼接逻辑
 * 3. 展示如何用静态数据模拟维表关联（生产环境可替换为 AsyncFunction）
 *
 * 与 SQL 版本的区别：
 * - DataStream API 提供更细粒度的控制
 * - 可以在 ProcessJoinFunction 中实现复杂的匹配和转换逻辑
 * - 适合需要自定义状态管理的高级场景
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class DataStreamWideTableExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ========== 1. 模拟订单流 ==========
        // 字段：(order_id, user_id, product_id, amount, event_time_ms)
        DataStream<Tuple5<String, Integer, Integer, Double, Long>> orderStream = env
            .fromElements(
                Tuple5.of("O001", 1, 101, 299.0,
                    Instant.parse("2024-01-01T10:00:00Z").toEpochMilli()),
                Tuple5.of("O002", 2, 102, 599.0,
                    Instant.parse("2024-01-01T10:05:00Z").toEpochMilli()),
                Tuple5.of("O003", 3, 103, 1299.0,
                    Instant.parse("2024-01-01T10:10:00Z").toEpochMilli())
            )
            .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.DOUBLE, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple5<String, Integer, Integer, Double, Long>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.f4)
            );

        // ========== 2. 模拟支付流 ==========
        // 字段：(pay_id, order_id, pay_channel, pay_amount, event_time_ms)
        DataStream<Tuple5<String, String, String, Double, Long>> paymentStream = env
            .fromElements(
                // O001 的支付，订单后 5 分钟（在 30 分钟范围内 ✅）
                Tuple5.of("P001", "O001", "ALIPAY", 299.0,
                    Instant.parse("2024-01-01T10:05:00Z").toEpochMilli()),
                // O002 的支付，订单后 10 分钟（在 30 分钟范围内 ✅）
                Tuple5.of("P002", "O002", "WECHAT", 599.0,
                    Instant.parse("2024-01-01T10:15:00Z").toEpochMilli()),
                // O003 的支付，订单后 45 分钟（超出 30 分钟范围 ❌）
                Tuple5.of("P003", "O003", "BANK", 1299.0,
                    Instant.parse("2024-01-01T10:55:00Z").toEpochMilli())
            )
            .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple5<String, String, String, Double, Long>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.f4)
            );

        // ========== 3. Interval Join：订单流 ⋈ 支付流 ==========
        // 时间范围：订单创建后 0~30 分钟内的支付
        orderStream
            .keyBy(order -> order.f0)  // 按 order_id 分组
            .intervalJoin(paymentStream.keyBy(payment -> payment.f1))  // 按 order_id 关联
            .between(Time.minutes(0), Time.minutes(30))  // 时间范围
            .process(new ProcessJoinFunction<
                    Tuple5<String, Integer, Integer, Double, Long>,
                    Tuple5<String, String, String, Double, Long>,
                    String>() {
                @Override
                public void processElement(
                        Tuple5<String, Integer, Integer, Double, Long> order,
                        Tuple5<String, String, String, Double, Long> payment,
                        Context ctx,
                        Collector<String> out) {

                    // 模拟维表关联（生产环境可替换为 AsyncFunction 查询 MySQL/Redis）
                    String userName = lookupUserName(order.f1);
                    String productName = lookupProductName(order.f2);

                    // 拼接宽表记录
                    out.collect(String.format(
                        "【宽表】订单=%s | 金额=%.2f | 支付渠道=%s | " +
                        "用户=%s | 商品=%s",
                        order.f0, order.f3, payment.f2,
                        userName, productName
                    ));
                }
            })
            .print("DWD 宽表");

        // 预期输出：
        // O001 ⋈ P001 ✅（5 分钟内）→ 输出宽表记录
        // O002 ⋈ P002 ✅（10 分钟内）→ 输出宽表记录
        // O003 ⋈ P003 ❌（45 分钟，超出范围，不输出）

        env.execute("DataStream Wide Table Example");
    }

    /**
     * 模拟用户维表查询
     * 生产环境应替换为 AsyncFunction + MySQL/Redis 查询
     */
    private static String lookupUserName(int userId) {
        switch (userId) {
            case 1: return "张三(北京)";
            case 2: return "李四(上海)";
            case 3: return "王五(广州)";
            default: return "未知用户";
        }
    }

    /**
     * 模拟商品维表查询
     * 生产环境应替换为 AsyncFunction + MySQL/Redis 查询
     */
    private static String lookupProductName(int productId) {
        switch (productId) {
            case 101: return "无线耳机(数码)";
            case 102: return "运动鞋(服饰)";
            case 103: return "智能手机(数码)";
            default: return "未知商品";
        }
    }
}
