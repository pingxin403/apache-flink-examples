package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 累积窗口 vs 滑动窗口对比示例
 * 
 * 功能说明:
 * 演示累积窗口和滑动窗口在"今日累计"场景下的差异
 */
public class CumulateVsHopExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建源表
        String createSourceTable = 
            "CREATE TABLE orders (" +
            "    order_id STRING," +
            "    amount DECIMAL(10, 2)," +
            "    order_time TIMESTAMP(3)," +
            "    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "    'connector' = 'datagen'," +
            "    'rows-per-second' = '10'," +
            "    'fields.order_id.kind' = 'sequence'," +
            "    'fields.order_id.start' = '1'," +
            "    'fields.order_id.end' = '1000'," +
            "    'fields.amount.min' = '10'," +
            "    'fields.amount.max' = '1000'" +
            ")";
        
        tableEnv.executeSql(createSourceTable);

        // 方案一:使用累积窗口(正确方案)
        System.out.println("=== 累积窗口结果(正确) ===");
        String cumulateQuery = 
            "SELECT " +
            "    DATE_FORMAT(window_start, 'HH:mm:ss') as start_time," +
            "    DATE_FORMAT(window_end, 'HH:mm:ss') as end_time," +
            "    SUM(amount) as total_sales," +
            "    COUNT(*) as order_count " +
            "FROM TABLE(" +
            "    CUMULATE(" +
            "        TABLE orders, " +
            "        DESCRIPTOR(order_time), " +
            "        INTERVAL '10' SECOND,  -- 每 10 秒刷新" +
            "        INTERVAL '1' MINUTE    -- 累计 1 分钟" +
            "    )" +
            ") " +
            "GROUP BY window_start, window_end";
        
        tableEnv.executeSql(cumulateQuery).print();

        // 方案二:使用滑动窗口(错误方案)
        System.out.println("\n=== 滑动窗口结果(错误) ===");
        String hopQuery = 
            "SELECT " +
            "    DATE_FORMAT(window_start, 'HH:mm:ss') as start_time," +
            "    DATE_FORMAT(window_end, 'HH:mm:ss') as end_time," +
            "    SUM(amount) as total_sales," +
            "    COUNT(*) as order_count " +
            "FROM TABLE(" +
            "    HOP(" +
            "        TABLE orders, " +
            "        DESCRIPTOR(order_time), " +
            "        INTERVAL '10' SECOND,  -- 每 10 秒滑动" +
            "        INTERVAL '1' MINUTE    -- 窗口大小 1 分钟" +
            "    )" +
            ") " +
            "GROUP BY window_start, window_end";
        
        tableEnv.executeSql(hopQuery).print();
    }
}
