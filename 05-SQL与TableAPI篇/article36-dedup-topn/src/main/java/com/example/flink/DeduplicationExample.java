package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时去重示例：使用 ROW_NUMBER() 实现订单数据去重
 *
 * 演示内容：
 * 1. 使用 DataGen 模拟含重复数据的订单流
 * 2. 通过 ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... ASC) 保留最早到达的记录
 * 3. 通过 ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) 保留最新状态的记录
 *
 * 核心原理：
 * - PARTITION BY 指定去重键（如 order_id）
 * - ORDER BY ASC 保留第一条（首次出现），ORDER BY DESC 保留最后一条（最新状态）
 * - WHERE row_num = 1 过滤掉重复数据
 * - Flink 对此模式有专门优化：每个去重键只维护 1 条记录，状态非常紧凑
 *
 * 适用场景：日志去重、订单去重、支付回调去重等明细级去重需求
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/deduplication/">
 *     Flink Deduplication</a>
 */
public class DeduplicationExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态 TTL 为 1 小时，过期后自动清理去重状态
        // 生产环境中应根据业务去重周期设置，如按天去重设为 24h
        tableEnv.getConfig().set("table.exec.state.ttl", "1h");

        // 1. 创建订单流（DataGen 模拟，order_id 长度较短以产生重复）
        // 注意：fields.order_id.length = 3 会生成较短的随机字符串，增加重复概率
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  amount DOUBLE," +
            "  status STRING," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +  // 处理时间，用于去重排序
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.order_id.length' = '3'," +       // 短字符串，增加重复概率
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '50'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '1000'," +
            "  'fields.status.length' = '3'" +
            ")"
        );

        // 2. 方案一：按处理时间去重，保留第一条到达的记录
        // 这是最常用的去重模式，适合日志去重、埋点去重等场景
        // Flink 会优化为只保留每个 order_id 的第一条记录，状态非常小
        System.out.println("=== 方案一：按处理时间去重（保留第一条） ===");
        tableEnv.executeSql(
            "SELECT order_id, user_id, amount, status, order_time " +
            "FROM (" +
            "  SELECT *," +
            "    ROW_NUMBER() OVER (" +
            "      PARTITION BY order_id " +     // 按 order_id 去重
            "      ORDER BY proc_time ASC" +     // 按处理时间升序，保留最早到达的
            "    ) AS row_num " +
            "  FROM orders" +
            ") WHERE row_num = 1"                // 只保留第一条
        ).print();

        // 3. 方案二：按事件时间去重，保留最新状态的记录（取消上面的注释运行此方案）
        // 适合订单状态同步场景：同一订单经历 创建→支付→发货 多次更新，保留最新状态
        // System.out.println("=== 方案二：按事件时间去重（保留最新状态） ===");
        // tableEnv.executeSql(
        //     "SELECT order_id, user_id, amount, status, order_time " +
        //     "FROM (" +
        //     "  SELECT *," +
        //     "    ROW_NUMBER() OVER (" +
        //     "      PARTITION BY order_id " +
        //     "      ORDER BY order_time DESC" +  // 按事件时间降序，保留最新的
        //     "    ) AS row_num " +
        //     "  FROM orders" +
        //     ") WHERE row_num = 1"
        // ).print();
    }
}
