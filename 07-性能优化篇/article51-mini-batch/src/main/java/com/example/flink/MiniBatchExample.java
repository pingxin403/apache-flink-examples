package com.example.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Mini-Batch 聚合示例
 *
 * 演示如何通过 Mini-Batch 配置降低状态更新频率、提升聚合吞吐量。
 * 同时展示 Mini-Batch 与 Local-Global 两阶段聚合的组合使用。
 *
 * 核心配置：
 *   - table.exec.mini-batch.enabled = true          开启攒批
 *   - table.exec.mini-batch.allow-latency = 5s      攒批最大等待时间
 *   - table.exec.mini-batch.size = 5000             每批最大记录数
 *   - table.optimizer.agg-phase-strategy = TWO_PHASE 开启 Local-Global 两阶段聚合
 *
 * 运行方式：
 *   mvn clean package
 *   flink run -c com.example.flink.MiniBatchExample target/article51-mini-batch-1.0-SNAPSHOT.jar
 *
 * 或在 IDE 中直接运行 main 方法（需要将 provided 依赖改为 compile）。
 */
public class MiniBatchExample {

    public static void main(String[] args) throws Exception {
        // ========== 1. 创建流执行环境和表环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 2. 配置 Mini-Batch 优化 ==========
        Configuration config = tableEnv.getConfig().getConfiguration();

        // 开启 Mini-Batch 聚合
        config.setString("table.exec.mini-batch.enabled", "true");

        // 攒批最大等待时间：5 秒内的数据会被攒成一批处理
        // 值越大，攒批效果越好，但端到端延迟也越高
        config.setString("table.exec.mini-batch.allow-latency", "5s");

        // 每批最大记录数：达到 5000 条也会触发处理
        // 这是一个安全阀，防止高流量时单批数据过大
        config.setString("table.exec.mini-batch.size", "5000");

        // 开启 Local-Global 两阶段聚合（依赖 Mini-Batch）
        // TWO_PHASE：强制使用两阶段聚合
        // AUTO：由优化器自动决定（推荐生产使用）
        config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        System.out.println("===== Mini-Batch 配置 =====");
        System.out.println("mini-batch.enabled     = true");
        System.out.println("mini-batch.allow-latency = 5s");
        System.out.println("mini-batch.size        = 5000");
        System.out.println("agg-phase-strategy     = TWO_PHASE");
        System.out.println();

        // ========== 3. 构造模拟订单数据 ==========
        // 模拟电商订单：Tuple3<订单ID, 商品ID, 订单金额>
        DataStream<Tuple3<String, String, Double>> orderStream = env.fromElements(
                // 热门商品 A：大量订单（模拟热点 Key）
                Tuple3.of("ORD001", "productA", 99.0),
                Tuple3.of("ORD002", "productA", 199.0),
                Tuple3.of("ORD003", "productA", 149.0),
                Tuple3.of("ORD004", "productA", 299.0),
                Tuple3.of("ORD005", "productA", 59.0),
                Tuple3.of("ORD006", "productA", 399.0),
                Tuple3.of("ORD007", "productA", 79.0),
                Tuple3.of("ORD008", "productA", 129.0),
                Tuple3.of("ORD009", "productA", 249.0),
                Tuple3.of("ORD010", "productA", 189.0),
                // 普通商品 B
                Tuple3.of("ORD011", "productB", 50.0),
                Tuple3.of("ORD012", "productB", 80.0),
                Tuple3.of("ORD013", "productB", 120.0),
                // 普通商品 C
                Tuple3.of("ORD014", "productC", 60.0),
                Tuple3.of("ORD015", "productC", 90.0),
                // 普通商品 D
                Tuple3.of("ORD016", "productD", 70.0)
        );

        // ========== 4. 将 DataStream 转为 Table ==========
        Table orderTable = tableEnv.fromDataStream(
                orderStream,
                $("f0").as("order_id"),
                $("f1").as("product_id"),
                $("f2").as("amount")
        );
        tableEnv.createTemporaryView("orders", orderTable);

        // ========== 5. 执行聚合查询 ==========
        // 这条 SQL 会被 Mini-Batch + Local-Global 优化：
        //   逻辑计划：GroupAggregate
        //   物理计划：LocalGroupAggregate → GlobalGroupAggregate
        //
        // 优化效果：
        //   - Mini-Batch：同一 Key 的多条数据攒成一批，减少状态读写
        //   - Local-Global：每个 subtask 先做局部聚合，减少 Shuffle 数据量
        String aggregationSql =
                "SELECT " +
                "  product_id, " +
                "  COUNT(*)    AS order_cnt, " +
                "  SUM(amount) AS total_amount, " +
                "  CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount " +
                "FROM orders " +
                "GROUP BY product_id";

        System.out.println("===== 执行聚合 SQL =====");
        System.out.println(aggregationSql);
        System.out.println();

        // 查看执行计划（可以看到 LocalGroupAggregate 和 GlobalGroupAggregate）
        Table resultTable = tableEnv.sqlQuery(aggregationSql);
        System.out.println("===== 执行计划 =====");
        System.out.println(resultTable.explain());

        // 输出结果
        tableEnv.toChangelogStream(resultTable).print("聚合结果");

        env.execute("Mini-Batch Aggregation Example");
    }
}
