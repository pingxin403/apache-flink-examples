package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL 实现实时 PV/UV 统计示例
 *
 * 演示内容：
 * 1. 使用 Tumble 窗口统计每分钟 PV（COUNT(*)）
 * 2. 使用 COUNT(DISTINCT) 精确统计每分钟 UV
 * 3. 同时输出 PV 和 UV，展示两者的计算差异
 *
 * 实现方式：
 *   - PV：简单计数，每条记录 +1
 *   - UV：精确去重，COUNT(DISTINCT user_id)
 *   - 适用于日活 < 100 万的中小规模场景
 *
 * 注意事项：
 * - COUNT(DISTINCT) 底层使用 SET 状态，内存与用户数成正比
 * - 大规模场景建议使用 HyperLogLog 近似去重（见 DataStreamHllUvExample）
 * - 本示例使用 DataGen 模拟数据，生产环境替换为 Kafka Source
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class SqlPvUvExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 开启 Mini-Batch 优化，减少状态更新频率
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");

        // ========== 第一步：创建页面访问流（DataGen 模拟） ==========
        tableEnv.executeSql(
            "CREATE TABLE page_view_stream (" +
            "  user_id INT," +
            "  page_id STRING," +
            "  event_time TIMESTAMP(3)," +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '100'," +  // 100 个用户，方便观察去重效果
            "  'fields.page_id.length' = '3'" +
            ")"
        );

        // ========== 第二步：PV 统计（每分钟窗口） ==========
        // PV = COUNT(*)，每条记录计一次
        System.out.println("===== 每分钟 PV 统计 =====");
        tableEnv.executeSql(
            "SELECT " +
            "  window_start," +
            "  window_end," +
            "  COUNT(*) AS pv " +
            "FROM TABLE(" +
            "  TUMBLE(TABLE page_view_stream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY window_start, window_end"
        ).print();
    }
}
