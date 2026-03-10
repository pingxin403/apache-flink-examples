package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 异步 Lookup Join 示例：使用 Query Hints 启用异步查询模式
 *
 * 演示内容：
 * 1. 使用 LOOKUP Hint 启用异步查询（async = true）
 * 2. 配置异步并发容量（capacity）和超时时间（timeout）
 * 3. 对比同步与异步 Lookup Join 的性能差异
 *
 * 异步 Lookup 的优势：
 * - 查询外部存储时不阻塞处理线程
 * - 可以同时发起多个并发查询
 * - 适合高流量、追求吞吐的场景
 *
 * 注意事项：
 * - 异步模式下数据可能乱序，需要根据业务决定是否启用有序模式
 * - capacity 参数控制最大并发查询数，设置过大可能压垮数据库
 * - 需要 Flink 1.16+ 版本支持
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class AsyncLookupJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建订单流（DataGen 模拟高流量数据）
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  amount DOUBLE," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '100'," +
            "  'fields.order_id.length' = '8'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '1000'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '500'" +
            ")"
        );

        // 2. 创建用户维表（JDBC + 缓存）
        tableEnv.executeSql(
            "CREATE TABLE user_dim (" +
            "  user_id INT," +
            "  user_name STRING," +
            "  user_level INT," +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'user_dim'," +
            "  'username' = 'root'," +
            "  'password' = '******'," +
            "  'lookup.cache' = 'PARTIAL'," +
            "  'lookup.partial-cache.max-rows' = '10000'," +
            "  'lookup.partial-cache.expire-after-write' = '120s'," +
            "  'lookup.max-retries' = '3'" +
            ")"
        );

        // 3. 异步 Lookup Join（使用 LOOKUP Hint）
        // 关键参数说明：
        //   - table='u'：指定维表别名
        //   - async='true'：启用异步查询模式
        //   - capacity='100'：最大并发查询数（根据数据库承受能力调整）
        //   - timeout='60s'：单次查询超时时间
        tableEnv.executeSql(
            "SELECT " +
            "  /*+ LOOKUP('table'='u', 'async'='true', 'capacity'='100', 'timeout'='60s') */ " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.amount," +
            "  o.order_time," +
            "  u.user_name," +
            "  u.user_level " +
            "FROM orders AS o " +
            "JOIN user_dim FOR SYSTEM_TIME AS OF o.proc_time AS u " +
            "  ON o.user_id = u.user_id"
        ).print();
    }
}
