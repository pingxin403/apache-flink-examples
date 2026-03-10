package com.example.flink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 幂等写入示例 - 三种实现方式
 * 
 * 1. INSERT IGNORE - 冲突时忽略
 * 2. REPLACE INTO - 冲突时删除后插入
 * 3. INSERT ... ON DUPLICATE KEY UPDATE - 冲突时更新
 */
public class IdempotentWriteExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(60000);

        DataStream<Order> orders = env.addSource(new OrderSource());

        // JDBC 连接配置
        String jdbcUrl = "jdbc:mysql://localhost:3306/flink_demo?" +
                "useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&" +
                "cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048";

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("password")
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(5000)
                .withMaxRetries(3)
                .build();

        // 方式一:INSERT IGNORE - 冲突时忽略(推荐用于只插入不更新的场景)
        orders.addSink(JdbcSink.sink(
                "INSERT IGNORE INTO orders (id, user_id, amount, create_time) VALUES (?, ?, ?, NOW())",
                (ps, order) -> {
                    ps.setString(1, order.getId());
                    ps.setString(2, order.getUserId());
                    ps.setDouble(3, order.getAmount());
                },
                executionOptions,
                connectionOptions
        )).name("INSERT IGNORE Sink");

        // 方式二:REPLACE INTO - 冲突时删除后插入(适用于完全替换的场景)
        // orders.addSink(JdbcSink.sink(
        //         "REPLACE INTO orders (id, user_id, amount, create_time) VALUES (?, ?, ?, NOW())",
        //         (ps, order) -> {
        //             ps.setString(1, order.getId());
        //             ps.setString(2, order.getUserId());
        //             ps.setDouble(3, order.getAmount());
        //         },
        //         executionOptions,
        //         connectionOptions
        // )).name("REPLACE INTO Sink");

        // 方式三:ON DUPLICATE KEY UPDATE - 冲突时更新(最灵活,推荐)
        // orders.addSink(JdbcSink.sink(
        //         "INSERT INTO orders (id, user_id, amount, create_time) VALUES (?, ?, ?, NOW()) " +
        //         "ON DUPLICATE KEY UPDATE amount = VALUES(amount), create_time = NOW()",
        //         (ps, order) -> {
        //             ps.setString(1, order.getId());
        //             ps.setString(2, order.getUserId());
        //             ps.setDouble(3, order.getAmount());
        //         },
        //         executionOptions,
        //         connectionOptions
        // )).name("ON DUPLICATE KEY UPDATE Sink");

        env.execute("JDBC Sink - Idempotent Write Example");
    }
}
