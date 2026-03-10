package com.example.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义 Sink 示例主程序。
 *
 * <p>演示如何使用 {@link TransactionalFileSink} 实现 Exactly-Once 语义的文件写入。
 * 数据先写入 .tmp 临时文件，Checkpoint 成功后原子重命名为 .data 正式文件。
 *
 * <p>运行方式：
 * <pre>
 * mvn clean package
 * flink run -c com.example.flink.CustomSinkExample \
 *   target/article31-custom-sink-1.0-SNAPSHOT.jar
 * </pre>
 *
 * <p>观察输出目录 /tmp/flink-sink-output：
 * <ul>
 *   <li>.tmp 文件：正在写入的数据（预提交阶段）</li>
 *   <li>.data 文件：已提交的数据（Checkpoint 成功后）</li>
 * </ul>
 */
public class CustomSinkExample {

    /** 输出目录（可通过命令行参数覆盖） */
    private static final String DEFAULT_OUTPUT_DIR = "/tmp/flink-sink-output";

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 开启 Checkpoint（两阶段提交必须开启 Checkpoint）
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        // 设置并行度为 2，演示多子任务独立管理事务
        env.setParallelism(2);

        // 3. 创建模拟数据源（每 2 秒生成一条 JSON 数据）
        DataStream<String> source = env
                .addSource(new SimulatedDeviceSource())
                .name("Simulated Device Source");

        // 4. 使用自定义事务 Sink 写入文件
        String outputDir = args.length > 0 ? args[0] : DEFAULT_OUTPUT_DIR;
        source.addSink(new TransactionalFileSink(outputDir))
              .name("Transactional File Sink");

        // 5. 执行作业
        env.execute("Custom Sink - TwoPhaseCommit Example");
    }

    /**
     * 模拟设备数据源：每 2 秒生成一条 JSON 格式的设备数据。
     * 每个并行实例生成不同 deviceId 的数据，避免重复。
     */
    static class SimulatedDeviceSource extends RichParallelSourceFunction<String> {

        private volatile boolean running = true;
        private int seq = 0;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

            while (running) {
                // 生成模拟设备数据（JSON 格式）
                double temperature = 20 + Math.random() * 30;
                String timestamp = java.time.LocalDateTime.now()
                        .format(java.time.format.DateTimeFormatter
                                .ofPattern("yyyy-MM-dd HH:mm:ss"));

                String json = String.format(
                        "{\"deviceId\":\"device-%d-%d\",\"temperature\":%.1f,"
                        + "\"timestamp\":\"%s\",\"seq\":%d}",
                        subtaskIdx, seq, temperature, timestamp, seq);

                ctx.collect(json);
                seq++;

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    if (!running) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
