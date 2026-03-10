package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 累积窗口示例:实现今日累计销售额统计
 * 
 * 功能说明:
 * 1. 从 Kafka 读取订单数据
 * 2. 使用累积窗口计算今日累计指标
 * 3. 每分钟刷新一次结果
 * 4. 将结果写入 MySQL
 */
public class CumulateWindowExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建源表:订单流(从 Kafka 读取)
        String createSourceTable = 
            "CREATE TABLE orders (" +
            "    order_id STRING," +
            "    user_id STRING," +
            "    product_id STRING," +
            "    amount DECIMAL(10, 2)," +
            "    order_time TIMESTAMP(3)," +
            "    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'orders'," +
            "    'properties.bootstrap.servers' = 'localhost:9092'," +
            "    'properties.group.id' = 'flink-cumulate-window'," +
            "    'scan.startup.mode' = 'latest-offset'," +
            "    'format' = 'json'," +
            "    'json.timestamp-format.standard' = 'ISO-8601'" +
            ")";
        
        tableEnv.executeSql(createSourceTable);

        // 3. 创建结果表:累计指标(写入 MySQL)
        String createSinkTable = 
            "CREATE TABLE daily_metrics (" +
            "    stat_date DATE," +
            "    stat_time TIME," +
            "    total_orders BIGINT," +
            "    total_sales DECIMAL(10, 2)," +
            "    active_users BIGINT," +
            "    PRIMARY KEY (stat_date, stat_time) NOT ENFORCED" +
            ") WITH (" +
            "    'connector' = 'jdbc'," +
            "    'url' = 'jdbc:mysql://localhost:3306/flink_demo'," +
            "    'table-name' = 'daily_metrics'," +
            "    'username' = 'root'," +
            "    'password' = 'password'" +
            ")";
        
        tableEnv.executeSql(createSinkTable);

        // 4. 使用累积窗口计算今日累计指标
        String cumulateQuery = 
            "INSERT INTO daily_metrics " +
            "SELECT " +
            "    CAST(window_start AS DATE) as stat_date," +
            "    CAST(window_end AS TIME) as stat_time," +
            "    COUNT(*) as total_orders," +
            "    SUM(amount) as total_sales," +
            "    COUNT(DISTINCT user_id) as active_users " +
            "FROM TABLE(" +
            "    CUMULATE(" +
            "        TABLE orders, " +
            "        DESCRIPTOR(order_time), " +
            "        INTERVAL '1' MINUTE,  -- 每分钟刷新一次" +
            "        INTERVAL '1' DAY      -- 累计一天的数据" +
            "    )" +
            ") " +
            "GROUP BY window_start, window_end";
        
        // 5. 执行查询
        tableEnv.executeSql(cumulateQuery);
        
        System.out.println("累积窗口作业已启动,正在计算今日累计指标...");
    }
}
