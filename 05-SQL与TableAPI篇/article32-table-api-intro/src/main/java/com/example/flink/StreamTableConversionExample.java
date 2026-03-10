package com.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * DataStream 与 Table 双向转换示例
 *
 * 本示例演示了流表互转的核心用法：
 * 1. fromDataStream：DataStream → Table（自动推断 / 手动指定 Schema）
 * 2. toDataStream：Table → DataStream（INSERT-only 场景）
 * 3. toChangelogStream：Table → DataStream（包含 UPDATE/DELETE 的场景）
 *
 * 场景：读取订单数据流，用 SQL 做聚合统计，再转回 DataStream 做后续处理
 *
 * @author 韩云朋
 */
public class StreamTableConversionExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 模拟订单数据流
        DataStream<Row> orderStream = env.fromElements(
                Row.of("order-001", "Alice", 99.9, 1000L),
                Row.of("order-002", "Bob", 199.0, 2000L),
                Row.of("order-003", "Alice", 50.0, 3000L),
                Row.of("order-004", "Charlie", 300.0, 4000L),
                Row.of("order-005", "Bob", 75.5, 5000L)
        ).returns(Types.ROW_NAMED(
                new String[]{"order_id", "user_name", "amount", "order_time"},
                Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG
        ));

        // ========== 3. DataStream → Table（手动指定 Schema） ==========
        // 使用 Schema.newBuilder() 可以精确控制字段名和类型
        Table orderTable = tableEnv.fromDataStream(orderStream,
                Schema.newBuilder()
                        .column("order_id", DataTypes.STRING())
                        .column("user_name", DataTypes.STRING())
                        .column("amount", DataTypes.DOUBLE())
                        .column("order_time", DataTypes.BIGINT())
                        .build()
        );

        // ========== 4. 简单过滤（INSERT-only，可用 toDataStream） ==========
        // 过滤金额大于 100 的订单
        Table highValueOrders = orderTable
                .filter($("amount").isGreater(100.0))
                .select($("order_id"), $("user_name"), $("amount"));

        // INSERT-only 的结果可以直接用 toDataStream
        DataStream<Row> highValueStream = tableEnv.toDataStream(highValueOrders);
        highValueStream.print("高价值订单");

        // ========== 5. 聚合查询（产生 Changelog，必须用 toChangelogStream） ==========
        tableEnv.createTemporaryView("orders", orderTable);
        Table userStats = tableEnv.sqlQuery(
                "SELECT user_name, " +
                "       COUNT(*) AS order_count, " +
                "       SUM(amount) AS total_amount " +
                "FROM orders GROUP BY user_name"
        );

        // 聚合结果包含 UPDATE 操作，必须使用 toChangelogStream
        DataStream<Row> statsStream = tableEnv.toChangelogStream(userStats);
        statsStream.print("用户统计");

        env.execute("Stream-Table Conversion Example");
    }
}
