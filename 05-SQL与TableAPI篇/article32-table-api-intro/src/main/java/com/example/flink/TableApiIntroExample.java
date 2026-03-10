package com.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 入门示例
 *
 * 本示例演示了 Table API 的核心用法：
 * 1. 创建 StreamTableEnvironment（Table API 入口）
 * 2. DataStream → Table 转换
 * 3. 使用 Table API 进行分组聚合查询
 * 4. 使用 SQL 进行等价查询
 * 5. Table → DataStream 转换（Changelog Stream）
 *
 * 运行后可以观察到 Changelog Stream 的输出格式：
 * +I 表示插入，-U 表示撤回旧值，+U 表示更新新值
 *
 * @author 韩云朋
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/overview/">
 *     Flink Table API & SQL 官方文档</a>
 */
public class TableApiIntroExample {

    public static void main(String[] args) throws Exception {
        // ========== 1. 创建执行环境 ==========
        // StreamTableEnvironment 同时支持 DataStream 和 Table API
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度为 1，方便观察输出顺序
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 2. 创建数据源（模拟用户点击事件） ==========
        DataStream<Row> clickStream = env.fromElements(
                Row.of("Alice", "/home", 1000L),
                Row.of("Bob", "/product", 2000L),
                Row.of("Alice", "/product", 3000L),
                Row.of("Alice", "/cart", 4000L),
                Row.of("Bob", "/home", 5000L),
                Row.of("Charlie", "/home", 6000L)
        ).returns(Types.ROW_NAMED(
                new String[]{"user_name", "url", "click_time"},
                Types.STRING, Types.STRING, Types.LONG
        ));

        // ========== 3. DataStream → Table ==========
        // fromDataStream 将 DataStream 转换为 Table 对象
        Table clickTable = tableEnv.fromDataStream(clickStream);

        // 注册为临时视图，供 SQL 查询使用
        tableEnv.createTemporaryView("clicks", clickTable);

        // ========== 4. 使用 Table API 查询 ==========
        // 按用户名分组，统计每个用户的点击次数
        Table tableApiResult = clickTable
                .groupBy($("user_name"))
                .select(
                        $("user_name"),
                        $("url").count().as("click_count")
                );

        // ========== 5. 使用 SQL 查询（等价写法） ==========
        Table sqlResult = tableEnv.sqlQuery(
                "SELECT user_name, COUNT(url) AS click_count " +
                "FROM clicks GROUP BY user_name"
        );

        // ========== 6. Table → DataStream 并输出 ==========
        // 聚合查询会产生更新流（Changelog），必须使用 toChangelogStream
        // +I = INSERT, -U = UPDATE_BEFORE（撤回旧值）, +U = UPDATE_AFTER（更新新值）
        tableEnv.toChangelogStream(tableApiResult).print("Table API");
        tableEnv.toChangelogStream(sqlResult).print("SQL");

        // 执行作业
        env.execute("Table API Intro Example");
    }
}
