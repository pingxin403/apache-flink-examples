package com.example.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink CDC MySQL 全量+增量同步示例
 *
 * <p>功能说明：
 * <ul>
 *   <li>使用 Flink CDC 连接 MySQL，监听 orders 表的变更</li>
 *   <li>首次启动时执行全量快照读取（无锁），然后自动切换到增量 binlog 读取</li>
 *   <li>基于 Checkpoint 实现 Exactly-Once 语义</li>
 *   <li>演示如何过滤和处理不同类型的变更事件</li>
 * </ul>
 *
 * <p>前置条件：
 * <ul>
 *   <li>MySQL 开启 binlog（binlog_format = ROW, binlog_row_image = FULL）</li>
 *   <li>创建 CDC 专用账号并授予 REPLICATION SLAVE/CLIENT 权限</li>
 *   <li>目标数据库和表已存在</li>
 * </ul>
 *
 * <p>运行方式：
 * <pre>
 *   mvn clean package
 *   flink run -c com.example.flink.MysqlCdcExample target/article28-flink-cdc-1.0-SNAPSHOT.jar
 * </pre>
 *
 * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/">Flink CDC 官方文档</a>
 */
public class MysqlCdcExample {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcExample.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Checkpoint（Flink CDC 必须开启 Checkpoint 才能保证 Exactly-Once）
        configureCheckpoint(env);

        // 3. 构建 MySQL CDC Source
        MySqlSource<String> source = buildMySqlSource();

        // 4. 将 CDC Source 添加到数据流
        DataStreamSource<String> cdcStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 5. 过滤出 INSERT 和 UPDATE 事件（忽略 DELETE）
        DataStream<String> filteredStream = cdcStream
                .filter(new CdcEventFilter());

        // 6. 处理变更数据
        // 实际场景中，这里可以写入 Kafka、Elasticsearch、数据湖等
        filteredStream
                .map(new CdcEventMapper())
                .print("CDC Event");

        // 7. 启动作业
        env.execute("Flink CDC MySQL Sync Example");
    }

    /**
     * 配置 Checkpoint 参数
     *
     * <p>关键参数说明：
     * <ul>
     *   <li>interval: Checkpoint 间隔，建议 60 秒</li>
     *   <li>mode: EXACTLY_ONCE 保证精确一次语义</li>
     *   <li>timeout: 超时时间，全量阶段可能较慢，建议设大一些</li>
     *   <li>minPause: 两次 Checkpoint 最小间隔，避免频繁触发</li>
     * </ul>
     */
    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        // 每 60 秒做一次 Checkpoint
        env.enableCheckpointing(60000);

        CheckpointConfig config = env.getCheckpointConfig();
        // 精确一次语义
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint 超时 10 分钟（全量阶段可能较慢）
        config.setCheckpointTimeout(600000);
        // 两次 Checkpoint 最小间隔 5 秒
        config.setMinPauseBetweenCheckpoints(5000);
        // 同时只允许 1 个 Checkpoint
        config.setMaxConcurrentCheckpoints(1);

        LOG.info("Checkpoint configured: interval=60s, mode=EXACTLY_ONCE, timeout=10min");
    }

    /**
     * 构建 MySQL CDC Source
     *
     * <p>核心配置说明：
     * <ul>
     *   <li>hostname/port: MySQL 连接地址，生产环境请使用配置文件或环境变量</li>
     *   <li>databaseList/tableList: 监听的数据库和表，支持正则匹配</li>
     *   <li>startupOptions: 启动模式，initial() 表示先全量后增量</li>
     *   <li>splitSize: 增量快照算法的 Chunk 大小，影响全量读取速度</li>
     *   <li>fetchSize: 每次从数据库拉取的行数</li>
     * </ul>
     */
    private static MySqlSource<String> buildMySqlSource() {
        return MySqlSource.<String>builder()
                // MySQL 连接配置（生产环境请使用配置文件或环境变量）
                .hostname("localhost")
                .port(3306)
                .username("flink_cdc")
                .password("your_password")
                // 监听的数据库和表（支持正则，如 "mydb\\.order.*" 匹配所有 order 开头的表）
                .databaseList("mydb")
                .tableList("mydb.orders")
                // 反序列化器：将 Debezium 变更事件转为 JSON 字符串
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 启动模式：initial 表示先全量快照后增量 binlog
                .startupOptions(StartupOptions.initial())
                // 增量快照算法配置
                .splitSize(8096)   // 每个 Chunk 的大小（行数），大表可调大到 100000
                .fetchSize(1024)   // 每次从数据库拉取的行数
                .build();
    }

    /**
     * CDC 事件过滤器
     *
     * <p>过滤逻辑：只保留 INSERT(c)、UPDATE(u) 和全量读取(r) 事件，
     * 过滤掉 DELETE(d) 事件。实际场景中可根据业务需求调整。
     */
    private static class CdcEventFilter implements FilterFunction<String> {
        @Override
        public boolean filter(String json) throws Exception {
            // 简单判断：如果包含 "op":"d" 则过滤掉删除事件
            // 生产环境建议使用 JSON 解析库进行精确判断
            return !json.contains("\"op\":\"d\"");
        }
    }

    /**
     * CDC 事件处理器
     *
     * <p>将原始 JSON 事件转换为简化格式，方便下游消费。
     * 实际场景中可以在这里做数据清洗、格式转换、字段映射等操作。
     */
    private static class CdcEventMapper implements MapFunction<String, String> {
        @Override
        public String map(String json) throws Exception {
            // 提取操作类型用于日志标记
            String opType = "UNKNOWN";
            if (json.contains("\"op\":\"c\"")) {
                opType = "INSERT";
            } else if (json.contains("\"op\":\"u\"")) {
                opType = "UPDATE";
            } else if (json.contains("\"op\":\"r\"")) {
                opType = "SNAPSHOT";
            }
            return String.format("[%s] %s", opType, json);
        }
    }
}
