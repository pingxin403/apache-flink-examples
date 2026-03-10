package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Lookup Join 示例：订单流关联用户维表（带缓存优化）
 *
 * 演示内容：
 * 1. 使用 DataGen 连接器模拟订单流
 * 2. 使用 JDBC 连接器定义用户维表（MySQL）
 * 3. 通过 FOR SYSTEM_TIME AS OF 语法实现 Lookup Join
 * 4. 配置 PARTIAL 缓存策略优化查询性能
 *
 * 前置条件：
 * - MySQL 中需要创建 flink_demo 数据库和 user_dim 表
 * - 建表 SQL 见 README.md
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class LookupJoinExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境和 Table 环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建订单流（DataGen 模拟数据）
        // proc_time 是处理时间属性，Lookup Join 必须使用处理时间
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.order_id.length' = '8'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '100'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '500'" +
            ")"
        );

        // 2. 创建用户维表（JDBC 连接 MySQL）
        // 关键配置：
        //   - lookup.cache = PARTIAL：启用部分缓存，按需加载
        //   - lookup.partial-cache.max-rows：缓存最大行数，防止内存溢出
        //   - lookup.partial-cache.expire-after-write：写入后过期时间
        //   - lookup.max-retries：查询失败重试次数
        tableEnv.executeSql(
            "CREATE TABLE user_dim (" +
            "  user_id INT," +
            "  user_name STRING," +
            "  user_level INT," +
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

        // 3. 执行 Lookup Join
        // FOR SYSTEM_TIME AS OF o.proc_time 表示在处理时间点查询维表快照
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.amount," +
            "  o.order_time," +
            "  u.user_name," +
            "  u.user_level " +
            "FROM orders AS o " +
            "JOIN user_dim FOR SYSTEM_TIME AS OF o.proc_time AS u " +
            "  ON o.user_id = u.user_id"
        ).print();
    }
}
