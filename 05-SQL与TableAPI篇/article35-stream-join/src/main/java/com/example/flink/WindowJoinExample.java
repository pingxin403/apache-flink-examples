package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Window Join 示例：订单流与支付流的 Tumble 窗口关联
 *
 * 演示内容：
 * 1. 使用 DataGen 模拟订单流和支付流
 * 2. 通过 Tumble Window Join 将同一 5 分钟窗口内的订单与支付匹配
 * 3. 窗口关闭时批量输出匹配结果
 *
 * 适用场景：两条流的数据天然按时间段聚合，如每 5 分钟的点击与曝光汇总
 */
public class WindowJoinExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建订单流（DataGen 模拟）
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.order_id.length' = '6'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '20'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '500'" +
            ")"
        );

        // 2. 创建支付流（DataGen 模拟）
        tableEnv.executeSql(
            "CREATE TABLE payments (" +
            "  pay_id STRING," +
            "  order_id STRING," +
            "  pay_amount DOUBLE," +
            "  pay_channel STRING," +
            "  pay_time TIMESTAMP(3)," +
            "  WATERMARK FOR pay_time AS pay_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '3'," +
            "  'fields.pay_id.length' = '8'," +
            "  'fields.order_id.length' = '6'," +
            "  'fields.pay_amount.min' = '10'," +
            "  'fields.pay_amount.max' = '500'," +
            "  'fields.pay_channel.length' = '4'" +
            ")"
        );

        // 3. Tumble Window Join：5 分钟窗口内的订单与支付匹配
        // 只有落在同一个 5 分钟窗口内且 order_id 相同的记录才能匹配
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.amount AS order_amount," +
            "  p.pay_id," +
            "  p.pay_amount," +
            "  p.pay_channel," +
            "  TUMBLE_START(o.order_time, INTERVAL '5' MINUTE) AS window_start," +
            "  TUMBLE_END(o.order_time, INTERVAL '5' MINUTE) AS window_end " +
            "FROM orders o, payments p " +
            "WHERE o.order_id = p.order_id " +
            "  AND TUMBLE(o.order_time, INTERVAL '5' MINUTE) = " +
            "      TUMBLE(p.pay_time, INTERVAL '5' MINUTE)"
        ).print();
    }
}
