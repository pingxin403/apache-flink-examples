package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL 实现 PV + UV 联合统计示例
 *
 * 演示内容：
 * 1. 在同一个 Tumble 窗口中同时计算 PV 和 UV
 * 2. 按页面维度分组，输出每个页面的 PV/UV 及人均访问次数
 * 3. 展示 Mini-Batch 优化配置
 *
 * 核心 SQL：
 *   SELECT page_id,
 *          COUNT(*)                AS pv,
 *          COUNT(DISTINCT user_id) AS uv,
 *          COUNT(*) / COUNT(DISTINCT user_id) AS avg_visit
 *   FROM TUMBLE(...)
 *   GROUP BY page_id, window_start, window_end
 *
 * 注意事项：
 * - COUNT(DISTINCT) 在 Flink SQL 中会自动使用状态去重
 * - 开启 Mini-Batch 可以合并短时间内的多次更新，提升吞吐
 * - 本示例使用 DataGen 模拟数据，生产环境替换为 Kafka Source
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class SqlPvUvCombinedExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 开启 Mini-Batch 优化
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");

        // 设置状态 TTL（UV 去重状态会随用户数增长）
        tableEnv.getConfig().set("table.exec.state.ttl", "1h");

        // ========== 第一步：创建页面访问流 ==========
        tableEnv.executeSql(
            "CREATE TABLE page_view_stream (" +
            "  user_id INT," +
            "  page_id STRING," +
            "  event_time TIMESTAMP(3)," +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '20'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '200'," +
            "  'fields.page_id.length' = '3'" +
            ")"
        );

        // ========== 第二步：PV + UV 联合统计 ==========
        // 在同一个窗口中同时计算 PV、UV 和人均访问次数
        System.out.println("===== 每分钟 PV/UV 联合统计（按页面维度） =====");
        tableEnv.executeSql(
            "SELECT " +
            "  window_start," +
            "  window_end," +
            "  page_id," +
            "  COUNT(*)                                    AS pv," +
            "  COUNT(DISTINCT user_id)                     AS uv," +
            "  CAST(COUNT(*) AS DOUBLE) " +
            "      / COUNT(DISTINCT user_id)               AS avg_visit_per_user " +
            "FROM TABLE(" +
            "  TUMBLE(TABLE page_view_stream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY window_start, window_end, page_id"
        ).print();
    }
}
