package com.example.flink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单模式示例 - 每条数据立即写入
 * 
 * 特点:
 * - 无批量,每条数据立即执行 SQL
 * - 延迟最低,但性能最差
 * - 适用于数据量很小的场景
 */
public class SimpleModeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建订单数据源
        DataStream<Order> orders = env.addSource(new OrderSource());

        // 配置 JDBC 连接
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("password")
                .build();

        // 简单模式:每条数据立即写入
        orders.addSink(JdbcSink.sink(
                "INSERT INTO orders (id, user_id, amount, create_time) VALUES (?, ?, ?, NOW())",
                (ps, order) -> {
                    ps.setString(1, order.getId());
                    ps.setString(2, order.getUserId());
                    ps.setDouble(3, order.getAmount());
                },
                connectionOptions
        ));

        env.execute("JDBC Sink - Simple Mode Example");
    }
}
