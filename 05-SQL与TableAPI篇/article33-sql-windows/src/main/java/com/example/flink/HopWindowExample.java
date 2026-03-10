package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Hop（滑动）窗口示例
 *
 * 演示使用 Flink SQL 的 TVF 语法实现滑动窗口聚合。
 * Hop 窗口允许窗口之间有重叠，需要指定滑动步长（slide）和窗口大小（size）。
 * 一条数据可能同时属于多个窗口，计算量 = 窗口大小 / 步长 × Tumble 的计算量。
 *
 * 场景：最近 1 分钟的平均订单金额，每 30 秒更新一次。
 *
 * @author 韩云朋
 */
public class HopWindowExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境和 Table 环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建模拟订单表
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id STRING," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.order_id.length' = '8'," +
            "  'fields.user_id.length' = '4'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '500'" +
            ")"
        );

        // 3. Hop 窗口聚合：最近 1 分钟的统计，每 30 秒滑动一次
        //    第三个参数是滑动步长（slide），第四个参数是窗口大小（size）
        //    每条数据会被计算 size/slide = 2 次
        tableEnv.executeSql(
            "SELECT " +
            "  window_start, " +
            "  window_end, " +
            "  COUNT(*) AS order_count, " +
            "  CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount, " +
            "  CAST(MAX(amount) AS DECIMAL(10,2)) AS max_amount " +
            "FROM TABLE(" +
            "  HOP(TABLE orders, DESCRIPTOR(order_time), " +
            "      INTERVAL '30' SECOND, INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY window_start, window_end"
        ).print();
    }
}
