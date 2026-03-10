package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时宽表构建示例：Interval Join + Lookup Join 组合
 *
 * 演示内容：
 * 1. 使用 Interval Join 关联订单流与支付流（流流 Join 拼事实）
 * 2. 使用 Lookup Join 关联用户维表和商品维表（维表 Join 补维度）
 * 3. 最终输出包含完整信息的 DWD 订单宽表
 *
 * 构建思路：
 *   订单流 ⋈ 支付流（Interval Join，0~30min）
 *       → 关联用户维表（Lookup Join）
 *       → 关联商品维表（Lookup Join）
 *       → DWD 订单宽表
 *
 * 注意事项：
 * - Interval Join 只保留时间范围内的状态，不会无限膨胀
 * - Lookup Join 需要配置缓存策略，减少对外部存储的查询压力
 * - 本示例使用 DataGen 模拟流数据，生产环境替换为 Kafka Source
 *
 * 前置条件（Lookup Join 部分）：
 * - MySQL 中需要创建 flink_demo 数据库和 user_dim、product_dim 表
 * - 建表 SQL 见 README.md
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class WideTableJoinExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态 TTL，略大于 Interval Join 的时间范围
        tableEnv.getConfig().set("table.exec.state.ttl", "35min");

        // ========== 第一步：定义数据源 ==========

        // 1. 创建订单流（DataGen 模拟，生产环境替换为 Kafka）
        tableEnv.executeSql(
            "CREATE TABLE order_stream (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  product_id INT," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +  // Lookup Join 需要处理时间
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.order_id.length' = '6'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '50'," +
            "  'fields.product_id.min' = '1'," +
            "  'fields.product_id.max' = '20'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '999'" +
            ")"
        );

        // 2. 创建支付流（DataGen 模拟）
        tableEnv.executeSql(
            "CREATE TABLE payment_stream (" +
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
            "  'fields.pay_amount.max' = '999'," +
            "  'fields.pay_channel.length' = '4'" +
            ")"
        );

        // 3. 创建用户维表（JDBC 连接 MySQL）
        // 配置 PARTIAL 缓存：LRU 淘汰 + 60 秒过期
        tableEnv.executeSql(
            "CREATE TABLE user_dim (" +
            "  user_id INT," +
            "  user_name STRING," +
            "  city STRING," +
            "  vip_level INT," +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'user_dim'," +
            "  'username' = 'root'," +
            "  'password' = '******'," +
            "  'lookup.cache' = 'PARTIAL'," +
            "  'lookup.partial-cache.max-rows' = '10000'," +
            "  'lookup.partial-cache.expire-after-write' = '60s'," +
            "  'lookup.max-retries' = '3'" +
            ")"
        );

        // 4. 创建商品维表（JDBC 连接 MySQL）
        tableEnv.executeSql(
            "CREATE TABLE product_dim (" +
            "  product_id INT," +
            "  product_name STRING," +
            "  category STRING," +
            "  brand STRING," +
            "  PRIMARY KEY (product_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'product_dim'," +
            "  'username' = 'root'," +
            "  'password' = '******'," +
            "  'lookup.cache' = 'PARTIAL'," +
            "  'lookup.partial-cache.max-rows' = '5000'," +
            "  'lookup.partial-cache.expire-after-write' = '120s'," +
            "  'lookup.max-retries' = '3'" +
            ")"
        );

        // ========== 第二步：Interval Join 订单流 ⋈ 支付流 ==========
        // 语义：订单创建后 0~30 分钟内的支付记录
        // 注册为临时视图，供后续 Lookup Join 使用
        tableEnv.executeSql(
            "CREATE TEMPORARY VIEW order_payment AS " +
            "SELECT " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.product_id," +
            "  o.amount       AS order_amount," +
            "  p.pay_id," +
            "  p.pay_channel," +
            "  p.pay_time," +
            "  o.order_time," +
            "  o.proc_time " +   // 传递处理时间给下游 Lookup Join
            "FROM order_stream o, payment_stream p " +
            "WHERE o.order_id = p.order_id " +
            "  AND p.pay_time BETWEEN o.order_time " +
            "      AND o.order_time + INTERVAL '30' MINUTE"
        );

        // ========== 第三步：Lookup Join 关联维表，输出宽表 ==========
        // 先关联用户维表，再关联商品维表
        // 最终输出完整的 DWD 订单宽表
        tableEnv.executeSql(
            "SELECT " +
            "  op.order_id," +
            "  op.order_amount," +
            "  op.pay_id," +
            "  op.pay_channel," +
            "  op.pay_time," +
            "  u.user_name," +
            "  u.city," +
            "  u.vip_level," +
            "  g.product_name," +
            "  g.category," +
            "  g.brand," +
            "  op.order_time " +
            "FROM order_payment AS op " +
            // Lookup Join 用户维表
            "JOIN user_dim FOR SYSTEM_TIME AS OF op.proc_time AS u " +
            "  ON op.user_id = u.user_id " +
            // Lookup Join 商品维表
            "JOIN product_dim FOR SYSTEM_TIME AS OF op.proc_time AS g " +
            "  ON op.product_id = g.product_id"
        ).print();
    }
}
