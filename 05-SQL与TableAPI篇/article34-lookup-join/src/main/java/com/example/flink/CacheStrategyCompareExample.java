package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 缓存策略对比示例：演示 NONE / PARTIAL / FULL 三种缓存模式
 *
 * 演示内容：
 * 1. NONE 模式：不缓存，每条数据都查外部存储
 * 2. PARTIAL 模式：部分缓存，按需加载 + LRU 淘汰（推荐）
 * 3. FULL 模式：全量缓存，启动时加载整张维表到内存
 *
 * 选型建议：
 * - 维表 < 10 万行 → FULL（查询最快，但占用内存）
 * - 维表 10 万 ~ 1000 万行 → PARTIAL（平衡性能与内存）
 * - 维表变化极频繁，要求强一致 → NONE（性能最差，但数据最新）
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class CacheStrategyCompareExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建订单流
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  product_id INT," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.order_id.length' = '8'," +
            "  'fields.product_id.min' = '1'," +
            "  'fields.product_id.max' = '50'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '500'" +
            ")"
        );

        // ========== 方式一：PARTIAL 缓存（推荐） ==========
        // 按需加载，LRU 淘汰，适合大多数场景
        tableEnv.executeSql(
            "CREATE TABLE product_dim_partial (" +
            "  product_id INT," +
            "  product_name STRING," +
            "  category STRING," +
            "  price DECIMAL(10, 2)," +
            "  PRIMARY KEY (product_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'product_dim'," +
            "  'username' = 'root'," +
            "  'password' = '******'," +
            "  'lookup.cache' = 'PARTIAL'," +
            "  'lookup.partial-cache.max-rows' = '5000'," +
            "  'lookup.partial-cache.expire-after-write' = '120s'," +
            "  'lookup.partial-cache.expire-after-access' = '60s'," +
            "  'lookup.max-retries' = '3'" +
            ")"
        );

        // ========== 方式二：FULL 缓存 ==========
        // 启动时全量加载，适合小维表（< 10 万行）
        // 通过 lookup.partial-cache.cache-missing-key 控制是否缓存空结果
        tableEnv.executeSql(
            "CREATE TABLE product_dim_full (" +
            "  product_id INT," +
            "  product_name STRING," +
            "  category STRING," +
            "  price DECIMAL(10, 2)," +
            "  PRIMARY KEY (product_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'product_dim'," +
            "  'username' = 'root'," +
            "  'password' = '******'," +
            "  'lookup.cache' = 'FULL'," +
            "  'lookup.full-cache.reload-strategy' = 'periodic'," +
            "  'lookup.full-cache.periodic-reload.interval' = '1h'," +
            "  'lookup.max-retries' = '3'" +
            ")"
        );

        // 使用 PARTIAL 缓存的维表执行 Lookup Join
        System.out.println("=== PARTIAL 缓存模式 Lookup Join ===");
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id," +
            "  o.product_id," +
            "  o.amount," +
            "  p.product_name," +
            "  p.category," +
            "  p.price " +
            "FROM orders AS o " +
            "JOIN product_dim_partial FOR SYSTEM_TIME AS OF o.proc_time AS p " +
            "  ON o.product_id = p.product_id"
        ).print();
    }
}
