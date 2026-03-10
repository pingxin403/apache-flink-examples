package com.example.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 执行模式示例：流模式 vs 批模式
 *
 * 本示例演示了 Table API 的两种执行模式：
 * 1. 流模式（Streaming Mode）：数据持续到达，结果增量更新
 * 2. 批模式（Batch Mode）：数据有界，一次性计算最终结果
 *
 * 通过 datagen 连接器生成模拟数据，展示两种模式下的行为差异。
 * 默认使用批模式运行，可修改 EnvironmentSettings 切换为流模式。
 *
 * @author 韩云朋
 */
public class ExecutionModeExample {

    public static void main(String[] args) {
        // ========== 批模式示例 ==========
        // 批模式适用于有界数据集，结果是最终结果（不产生 Changelog）
        System.out.println("===== 批模式（Batch Mode） =====");
        runBatchMode();

        // ========== 流模式示例 ==========
        // 流模式适用于无界数据流，结果是增量更新的
        System.out.println("\n===== 流模式（Streaming Mode） =====");
        runStreamingMode();
    }

    /**
     * 批模式：数据有界，一次性计算最终结果
     * 特点：支持全局排序、不需要水位线、执行效率高
     */
    private static void runBatchMode() {
        // 创建批模式的 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 使用 datagen 连接器生成有界数据（number-of-rows 限制数据量）
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE orders (" +
                "  order_id INT," +
                "  user_name STRING," +
                "  amount DOUBLE," +
                "  order_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'number-of-rows' = '20'," +       // 有界：只生成 20 行
                "  'fields.order_id.min' = '1'," +
                "  'fields.order_id.max' = '100'," +
                "  'fields.user_name.length' = '5'," +
                "  'fields.amount.min' = '10'," +
                "  'fields.amount.max' = '500'" +
                ")"
        );

        // 批模式下可以使用 ORDER BY（全局排序）
        TableResult result = tableEnv.executeSql(
                "SELECT user_name, " +
                "       COUNT(*) AS order_count, " +
                "       ROUND(SUM(amount), 2) AS total_amount " +
                "FROM orders " +
                "GROUP BY user_name " +
                "ORDER BY total_amount DESC"  // 批模式支持全局排序
        );

        // 打印最终结果
        result.print();
    }

    /**
     * 流模式：数据无界，结果持续更新
     * 特点：低延迟、增量更新、需要处理水位线
     */
    private static void runStreamingMode() {
        // 创建流模式的 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 使用 datagen 连接器生成有界数据（流模式也可以处理有界数据）
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE clicks (" +
                "  user_name STRING," +
                "  url STRING," +
                "  click_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'number-of-rows' = '10'," +
                "  'fields.user_name.length' = '4'," +
                "  'fields.url.length' = '8'" +
                ")"
        );

        // 流模式下的聚合查询（结果是 Changelog 形式）
        // 注意：流模式不支持 ORDER BY（因为数据是无界的）
        TableResult result = tableEnv.executeSql(
                "SELECT user_name, " +
                "       COUNT(*) AS click_count " +
                "FROM clicks " +
                "GROUP BY user_name"
        );

        // 打印结果（会看到 +I, -U, +U 等 Changelog 标记）
        result.print();
    }
}
