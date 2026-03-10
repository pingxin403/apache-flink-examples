package com.example.flink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 批量模式示例 - 批量写入,性能最优
 * 
 * 特点:
 * - 积攒一批数据后批量执行 SQL
 * - 性能好,延迟可接受
 * - 适用于大多数生产场景
 */
public class BatchModeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);  // 设置并行度为 4

        // 启用 Checkpoint
        env.enableCheckpointing(60000);  // 每 60 秒做一次 Checkpoint

        // 创建订单数据源
        DataStream<Order> orders = env.addSource(new OrderSource());

        // 配置 JDBC 连接(开启批量重写等优化参数)
        String jdbcUrl = "jdbc:mysql://localhost:3306/flink_demo?" +
                "useSSL=false&" +
                "serverTimezone=UTC&" +
                "rewriteBatchedStatements=true&" +  // ✅ 批量重写,性能提升 10 倍+
                "cachePrepStmts=true&" +             // ✅ 缓存预编译语句
                "prepStmtCacheSize=250&" +
                "prepStmtCacheSqlLimit=2048";

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("password")
                .build();

        // 配置批量执行参数
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)              // ✅ 每 1000 条提交一次
                .withBatchIntervalMs(5000)        // ✅ 或每 5 秒提交一次
                .withMaxRetries(3)                // ✅ 失败重试 3 次
                .build();

        // 批量模式:使用幂等写入(INSERT IGNORE)
        orders.addSink(JdbcSink.sink(
                "INSERT IGNORE INTO orders (id, user_id, amount, create_time) VALUES (?, ?, ?, NOW())",
                (ps, order) -> {
                    ps.setString(1, order.getId());
                    ps.setString(2, order.getUserId());
                    ps.setDouble(3, order.getAmount());
                },
                executionOptions,
                connectionOptions
        ));

        env.execute("JDBC Sink - Batch Mode Example");
    }
}
