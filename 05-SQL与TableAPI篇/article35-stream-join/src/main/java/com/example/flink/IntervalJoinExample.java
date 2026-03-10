package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Interval Join 示例：订单流与支付流的时间范围关联
 *
 * 演示内容：
 * 1. 使用 DataGen 模拟订单流和支付流
 * 2. 通过 Interval Join 匹配订单后 0~15 分钟内的支付记录
 * 3. 匹配成功后立即输出，无需等窗口关闭
 *
 * 适用场景：两条流的数据有因果时序关系，如下单后 N 分钟内支付
 */
public class IntervalJoinExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态 TTL，略大于 Join 时间范围，避免状态无限膨胀
        tableEnv.getConfig().set("table.exec.state.ttl", "16min");

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

        // 3. Interval Join：订单后 0~15 分钟内的支付
        // 语义：对于每条订单，在支付流中查找 order_time 到 order_time + 15min 范围内的支付
        // 匹配成功后立即输出，延迟远低于 Window Join
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.amount AS order_amount," +
            "  p.pay_id," +
            "  p.pay_amount," +
            "  p.pay_channel," +
            "  o.order_time," +
            "  p.pay_time " +
            "FROM orders o, payments p " +
            "WHERE o.order_id = p.order_id " +
            "  AND p.pay_time BETWEEN o.order_time " +
            "      AND o.order_time + INTERVAL '15' MINUTE"
        ).print();
    }
}
