package com.example.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义 Source 使用示例 —— 演示 HttpPollingSource 的完整用法
 *
 * <p>本示例展示：
 * <ul>
 *   <li>如何配置 Checkpoint 以支持 Exactly-Once 语义</li>
 *   <li>如何创建并使用自定义的 HttpPollingSource</li>
 *   <li>如何设置并行度控制数据分片</li>
 * </ul>
 *
 * <p>运行方式：
 * <pre>
 *   mvn clean package
 *   flink run -c com.example.flink.CustomSourceExample \
 *       target/article30-custom-source-1.0-SNAPSHOT.jar
 * </pre>
 *
 * <p>注意：本示例使用模拟 Source（SimulatedHttpPollingSource）替代真实 HTTP 请求，
 * 方便本地测试。生产环境请替换为 HttpPollingSource 并配置真实的 HTTP 地址。
 *
 * @see HttpPollingSource
 * @see SimulatedHttpPollingSource
 */
public class CustomSourceExample {

    private static final Logger LOG = LoggerFactory.getLogger(CustomSourceExample.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Checkpoint
        configureCheckpoint(env);

        // 3. 使用模拟 Source（本地测试用，无需真实 HTTP 服务）
        // 生产环境替换为：
        // new HttpPollingSource("http://your-api.com/data", 5000, 3000)
        DataStream<String> sourceStream = env
                .addSource(new SimulatedHttpPollingSource(2000))
                .setParallelism(2)  // 设置并行度为 2，模拟分片消费
                .name("Custom HTTP Polling Source");

        // 4. 简单处理：添加时间戳和子任务标识
        DataStream<String> processedStream = sourceStream
                .map(data -> String.format("[%tT] %s", System.currentTimeMillis(), data))
                .name("Add Timestamp");

        // 5. 输出结果
        processedStream.print("Output");

        // 6. 启动作业
        LOG.info("Starting Custom Source Example...");
        env.execute("Custom Source Example - HTTP Polling");
    }

    /**
     * 配置 Checkpoint 参数
     *
     * <p>关键参数说明：
     * <ul>
     *   <li>interval: 10 秒做一次 Checkpoint（演示用，生产建议 60 秒）</li>
     *   <li>mode: EXACTLY_ONCE 保证精确一次语义</li>
     *   <li>timeout: 超时 60 秒</li>
     *   <li>minPause: 两次 Checkpoint 最小间隔 3 秒</li>
     * </ul>
     */
    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        // 每 10 秒做一次 Checkpoint（演示用，生产建议 60 秒）
        env.enableCheckpointing(10000);

        CheckpointConfig config = env.getCheckpointConfig();
        // 精确一次语义
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint 超时 60 秒
        config.setCheckpointTimeout(60000);
        // 两次 Checkpoint 最小间隔 3 秒
        config.setMinPauseBetweenCheckpoints(3000);
        // 同时只允许 1 个 Checkpoint
        config.setMaxConcurrentCheckpoints(1);

        LOG.info("Checkpoint configured: interval=10s, mode=EXACTLY_ONCE");
    }
}
