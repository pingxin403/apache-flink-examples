package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时 TOP-N 示例：使用 ROW_NUMBER() 实现实时排行榜
 *
 * 演示内容：
 * 1. 使用 DataGen 模拟商品销售订单流
 * 2. 先按商品聚合销量（SUM），再对聚合结果做 TOP-N
 * 3. 实时输出每个品类销量前 3 的商品排行榜
 *
 * 核心原理：
 * - 第一步：GROUP BY 聚合，计算每个商品的累计销量
 * - 第二步：ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC)
 * - 第三步：WHERE row_num <= N 取前 N 名
 * - 当排名变化时，Flink 以 Changelog 形式输出（INSERT/UPDATE/DELETE）
 *
 * 最佳实践：
 * - 先聚合再 TOP-N，避免对明细数据直接排序（状态和计算开销巨大）
 * - 下游使用 Upsert 模式写入（如 Upsert-Kafka、MySQL），正确处理回撤消息
 *
 * 适用场景：商品销量排行、用户活跃度排名、热搜榜、实时大屏
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/topn/">
 *     Flink Top-N</a>
 */
public class TopNExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态 TTL，根据业务需求调整
        // TOP-N 的状态包含每个分组的前 N 条记录，TTL 过期后排名会重新计算
        tableEnv.getConfig().set("table.exec.state.ttl", "1h");

        // 开启 Mini-Batch 优化，减少状态更新频率，提升吞吐量
        // 在高 QPS 场景下，Mini-Batch 可以将多条更新合并为一次状态操作
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "1000");

        // 1. 创建商品销售订单流（DataGen 模拟）
        // category 使用短字符串模拟品类（如 "AB", "CD"），product_id 模拟商品
        tableEnv.executeSql(
            "CREATE TABLE order_items (" +
            "  order_id STRING," +
            "  product_id STRING," +
            "  category STRING," +
            "  quantity INT," +
            "  price DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '20'," +
            "  'fields.order_id.length' = '8'," +
            "  'fields.product_id.length' = '3'," +   // 短字符串，模拟有限的商品集合
            "  'fields.category.length' = '2'," +      // 短字符串，模拟有限的品类集合
            "  'fields.quantity.min' = '1'," +
            "  'fields.quantity.max' = '10'," +
            "  'fields.price.min' = '10'," +
            "  'fields.price.max' = '500'" +
            ")"
        );

        // 2. 第一步：按商品聚合销量
        // 创建视图，计算每个品类下每个商品的累计销量和销售额
        // 这一步将明细数据聚合为商品维度的指标，大幅减少后续 TOP-N 的输入量
        tableEnv.executeSql(
            "CREATE VIEW product_sales AS " +
            "SELECT " +
            "  category," +
            "  product_id," +
            "  SUM(quantity) AS total_quantity," +
            "  SUM(price * quantity) AS total_amount," +
            "  COUNT(*) AS order_count " +
            "FROM order_items " +
            "GROUP BY category, product_id"
        );

        // 3. 第二步：对聚合结果做 TOP-N，取每个品类销量前 3 的商品
        // ROW_NUMBER() 按 total_quantity 降序排列，WHERE row_num <= 3 取前 3 名
        // 当排名变化时，Flink 会输出 Changelog 事件：
        //   - 新商品进入前 3：INSERT
        //   - 已有商品销量更新：UPDATE_BEFORE + UPDATE_AFTER
        //   - 商品被挤出前 3：DELETE
        System.out.println("=== 每个品类销量 TOP 3 商品（实时更新） ===");
        tableEnv.executeSql(
            "SELECT category, product_id, total_quantity, " +
            "       total_amount, order_count, row_num " +
            "FROM (" +
            "  SELECT *," +
            "    ROW_NUMBER() OVER (" +
            "      PARTITION BY category " +          // 按品类分组
            "      ORDER BY total_quantity DESC" +     // 按销量降序
            "    ) AS row_num " +
            "  FROM product_sales" +
            ") WHERE row_num <= 3"                     // 取前 3 名
        ).print();
    }
}
