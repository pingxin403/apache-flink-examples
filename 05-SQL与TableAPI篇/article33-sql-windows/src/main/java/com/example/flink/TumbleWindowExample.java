package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Tumble（滚动）窗口示例
 *
 * 演示使用 Flink SQL 的 TVF 语法实现每分钟订单聚合统计。
 * Tumble 窗口将时间轴切成等长、不重叠的片段，每条数据只属于一个窗口。
 *
 * 运行方式：
 *   mvn clean package
 *   java -cp target/article33-sql-windows-1.0-SNAPSHOT.jar com.example.flink.TumbleWindowExample
 *
 * @author 韩云朋
 */
public class TumbleWindowExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境和 Table 环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 使用 DataGen 连接器创建模拟订单表
        //    - order_time 声明为事件时间，并设置 5 秒的水位线延迟
        //    - rows-per-second 控制数据生成速率
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

        // 3. Tumble 窗口聚合：每分钟统计订单数量、总金额、平均金额
        //    使用 TVF 语法（FROM TABLE(TUMBLE(...))），Flink 1.13+ 推荐写法
        tableEnv.executeSql(
            "SELECT " +
            "  window_start, " +
            "  window_end, " +
            "  COUNT(*) AS order_count, " +
            "  CAST(SUM(amount) AS DECIMAL(10,2)) AS total_amount, " +
            "  CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount " +
            "FROM TABLE(" +
            "  TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY window_start, window_end"
        ).print();
    }
}
