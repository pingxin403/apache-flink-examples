package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Regular Join 构建宽表示例
 *
 * 演示内容：
 * 1. 使用 Regular Join（无时间约束）关联订单流与物流流
 * 2. 展示 Regular Join 的 Changelog 输出特性（含 Retract 消息）
 * 3. 演示状态 TTL 的必要性——防止状态无限膨胀
 *
 * 适用场景：
 * - 两条流之间没有明确的时间因果关系
 * - 需要全量匹配（任何时刻的数据都可能关联上）
 * - 可以接受 Changelog（Retract）输出模式
 *
 * 注意事项：
 * - Regular Join 会保留双流的全量状态，必须设置 TTL
 * - 输出是 Changelog 流，下游 Sink 需要支持 Upsert 语义
 * - 建议开启 Mini-Batch 合并短时间内的多次更新
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class RegularJoinWideTableExample {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 关键配置 ==========
        // 1. 状态 TTL：Regular Join 必须设置，否则状态会无限增长导致 OOM
        //    设置为 1 小时，表示超过 1 小时未匹配的数据会被清理
        tableEnv.getConfig().set("table.exec.state.ttl", "1h");

        // 2. Mini-Batch 优化：合并短时间内的多次 Retract，减少下游压力
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");

        // 1. 创建订单流
        tableEnv.executeSql(
            "CREATE TABLE order_stream (" +
            "  order_id STRING," +
            "  user_id INT," +
            "  amount DOUBLE," +
            "  order_status STRING," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '3'," +
            "  'fields.order_id.length' = '6'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '30'," +
            "  'fields.amount.min' = '50'," +
            "  'fields.amount.max' = '800'," +
            "  'fields.order_status.length' = '3'" +
            ")"
        );

        // 2. 创建物流流（与订单没有严格的时间因果关系）
        tableEnv.executeSql(
            "CREATE TABLE logistics_stream (" +
            "  logistics_id STRING," +
            "  order_id STRING," +
            "  carrier STRING," +
            "  logistics_status STRING," +
            "  update_time TIMESTAMP(3)," +
            "  WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '2'," +
            "  'fields.logistics_id.length' = '8'," +
            "  'fields.order_id.length' = '6'," +
            "  'fields.carrier.length' = '4'," +
            "  'fields.logistics_status.length' = '3'" +
            ")"
        );

        // 3. Regular Join：订单流 ⋈ 物流流
        // 无时间约束，任何时刻的订单都可能匹配到物流信息
        // 输出是 Changelog 流：当物流状态更新时，会先 Retract 旧记录再 Insert 新记录
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id," +
            "  o.user_id," +
            "  o.amount," +
            "  o.order_status," +
            "  o.order_time," +
            "  l.logistics_id," +
            "  l.carrier," +
            "  l.logistics_status," +
            "  l.update_time AS logistics_time " +
            "FROM order_stream o " +
            "JOIN logistics_stream l " +
            "  ON o.order_id = l.order_id"
        ).print();

        // 输出说明：
        // +I 表示 INSERT（新增匹配记录）
        // -U 表示 UPDATE_BEFORE（撤回旧记录）
        // +U 表示 UPDATE_AFTER（插入更新后的记录）
        // 下游 Sink 需要支持 Upsert 语义（如 MySQL 的 INSERT ON DUPLICATE KEY UPDATE）
    }
}
