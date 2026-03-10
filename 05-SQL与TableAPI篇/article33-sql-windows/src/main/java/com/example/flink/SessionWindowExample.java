package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Session（会话）窗口示例
 *
 * 演示使用 Flink SQL 的旧版 GROUP WINDOW 语法实现会话窗口聚合。
 * Session 窗口根据数据的活跃间隔（gap）来划分窗口，
 * 如果两条数据之间的时间间隔超过 gap，就会被分到不同的窗口。
 *
 * 注意：Session 窗口的 TVF 语法在 Flink 1.17 中仍处于实验阶段，
 * 生产环境建议使用旧版 GROUP WINDOW 语法。
 *
 * 场景：用户会话分析，会话间隔 10 秒（演示用，实际场景通常为 30 分钟）。
 *
 * @author 韩云朋
 */
public class SessionWindowExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境和 Table 环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建模拟用户点击表
        tableEnv.executeSql(
            "CREATE TABLE user_clicks (" +
            "  user_id STRING," +
            "  page_url STRING," +
            "  click_time TIMESTAMP(3)," +
            "  WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.user_id.length' = '3'," +
            "  'fields.page_url.length' = '6'" +
            ")"
        );

        // 3. Session 窗口聚合：使用旧版 GROUP WINDOW 语法
        //    SESSION(click_time, INTERVAL '10' SECOND) 表示会话间隔为 10 秒
        //    SESSION_START / SESSION_END 获取会话的起止时间
        tableEnv.executeSql(
            "SELECT " +
            "  user_id, " +
            "  SESSION_START(click_time, INTERVAL '10' SECOND) AS session_start, " +
            "  SESSION_END(click_time, INTERVAL '10' SECOND) AS session_end, " +
            "  COUNT(*) AS page_views " +
            "FROM user_clicks " +
            "GROUP BY user_id, SESSION(click_time, INTERVAL '10' SECOND)"
        ).print();
    }
}
