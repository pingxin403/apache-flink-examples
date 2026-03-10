package com.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * 自定义 HTTP 轮询 Source —— 支持 Checkpoint 和并行度控制
 *
 * <p>功能说明：
 * <ul>
 *   <li>定时轮询指定 HTTP 接口，将返回的 JSON 数据发送到下游</li>
 *   <li>通过 CheckpointedFunction 集成 Checkpoint，记录已处理的偏移量</li>
 *   <li>故障恢复时从上次 Checkpoint 的偏移量继续消费</li>
 *   <li>支持并行执行，每个实例根据 subtaskIndex 请求不同的数据分片</li>
 * </ul>
 *
 * <p>核心机制：
 * <ul>
 *   <li>snapshotState: Checkpoint 触发时保存当前偏移量</li>
 *   <li>initializeState: 作业启动或恢复时读取偏移量</li>
 *   <li>CheckpointLock: 保证数据发送与状态更新的原子性</li>
 * </ul>
 *
 * @see org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
 */
public class HttpPollingSource extends RichParallelSourceFunction<String>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HttpPollingSource.class);

    /** HTTP 接口地址 */
    private final String url;

    /** 轮询间隔（毫秒） */
    private final long intervalMs;

    /** HTTP 请求超时时间（毫秒），建议小于 Checkpoint 间隔的 1/3 */
    private final int httpTimeoutMs;

    /** 控制 Source 运行状态，cancel() 时设为 false */
    private volatile boolean running = true;

    /** 当前偏移量（已处理的记录数） */
    private long currentOffset = 0;

    /** Checkpoint 状态存储，用于保存和恢复偏移量 */
    private transient ListState<Long> offsetState;

    /**
     * 构造函数
     *
     * @param url           HTTP 接口地址
     * @param intervalMs    轮询间隔（毫秒），建议 >= 1000
     * @param httpTimeoutMs HTTP 超时时间（毫秒），建议 < Checkpoint 间隔的 1/3
     */
    public HttpPollingSource(String url, long intervalMs, int httpTimeoutMs) {
        this.url = url;
        this.intervalMs = intervalMs;
        this.httpTimeoutMs = httpTimeoutMs;
    }

    /**
     * 状态初始化 —— 作业启动或从 Checkpoint/Savepoint 恢复时调用
     *
     * <p>关键逻辑：
     * <ul>
     *   <li>首次启动：offsetState 为空，currentOffset 保持默认值 0</li>
     *   <li>故障恢复：从 offsetState 中读取上次保存的偏移量</li>
     * </ul>
     *
     * <p>状态重分布策略：使用 getListState()（Even-split），
     * 当并行度变化时状态会均匀分配到新实例。
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 定义状态描述符
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>("http-polling-offset", Long.class);

        // 获取 Operator State（Even-split 模式）
        offsetState = context.getOperatorStateStore().getListState(descriptor);

        // 如果是从 Checkpoint/Savepoint 恢复，读取之前的偏移量
        if (context.isRestored()) {
            for (Long offset : offsetState.get()) {
                currentOffset = offset;
            }
            LOG.info("Subtask {} restored offset to {}",
                    getRuntimeContext().getIndexOfThisSubtask(), currentOffset);
        } else {
            LOG.info("Subtask {} starting fresh with offset 0",
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    /**
     * Checkpoint 快照 —— Checkpoint 触发时调用
     *
     * <p>将当前偏移量保存到状态中。注意：此方法在持有 CheckpointLock 的情况下被调用，
     * 因此与 run() 中的 synchronized 块互斥，保证状态一致性。
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(currentOffset);
        LOG.debug("Subtask {} snapshot offset {} at checkpoint {}",
                getRuntimeContext().getIndexOfThisSubtask(),
                currentOffset,
                context.getCheckpointId());
    }

    /**
     * 数据生产主循环
     *
     * <p>关键点：
     * <ul>
     *   <li>使用 subtaskIndex 和 numParallelSubtasks 实现数据分片</li>
     *   <li>数据发送和偏移量更新在 CheckpointLock 内完成，保证原子性</li>
     *   <li>HTTP 请求失败时进行重试，避免一次失败导致作业重启</li>
     *   <li>正确处理 InterruptedException，确保 cancel 后能及时退出</li>
     * </ul>
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        LOG.info("HttpPollingSource subtask {}/{} started, polling {} every {}ms",
                subtaskIndex, numParallelSubtasks, url, intervalMs);

        while (running) {
            try {
                // 构建带分片参数的请求 URL
                String requestUrl = String.format("%s?partition=%d&totalPartitions=%d&offset=%d",
                        url, subtaskIndex, numParallelSubtasks, currentOffset);

                // 发起 HTTP 请求（带超时和重试）
                String response = httpGetWithRetry(requestUrl, 3);

                if (response != null && !response.isEmpty()) {
                    // 加锁保证数据发送和偏移量更新的原子性
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(response);
                        currentOffset++;
                    }
                }
            } catch (Exception e) {
                if (!running) {
                    // cancel 已被调用，正常退出
                    break;
                }
                LOG.error("Subtask {} error polling {}: {}",
                        subtaskIndex, url, e.getMessage());
            }

            // 等待下一次轮询
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                if (!running) {
                    // cancel 已被调用，正常退出
                    break;
                }
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("HttpPollingSource subtask {} stopped", subtaskIndex);
    }

    /**
     * 取消 Source —— 作业停止时由 Flink 框架调用
     *
     * <p>设置 running = false，run() 方法会在下一次循环检查时退出。
     * 如果 run() 正在 sleep，InterruptedException 会被捕获并退出。
     */
    @Override
    public void cancel() {
        running = false;
        LOG.info("HttpPollingSource subtask {} cancel requested",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    /**
     * 带重试的 HTTP GET 请求
     *
     * @param requestUrl 请求地址
     * @param maxRetries 最大重试次数
     * @return 响应内容，失败返回 null
     */
    private String httpGetWithRetry(String requestUrl, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return httpGet(requestUrl);
            } catch (Exception e) {
                LOG.warn("HTTP request attempt {}/{} failed: {}",
                        attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        // 指数退避：1s, 2s, 4s...
                        Thread.sleep(1000L * (1L << (attempt - 1)));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 发起 HTTP GET 请求
     *
     * @param requestUrl 请求地址
     * @return 响应体字符串
     * @throws Exception 请求失败时抛出异常
     */
    private String httpGet(String requestUrl) throws Exception {
        URL urlObj = new URL(requestUrl);
        HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
        try {
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(httpTimeoutMs);
            conn.setReadTimeout(httpTimeoutMs);
            conn.setRequestProperty("Accept", "application/json");

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("HTTP request failed with code: " + responseCode);
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }
}
