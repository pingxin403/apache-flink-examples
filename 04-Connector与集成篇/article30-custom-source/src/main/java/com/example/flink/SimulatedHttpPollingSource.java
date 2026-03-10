package com.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Random;

/**
 * 模拟 HTTP 轮询 Source —— 用于本地测试，无需真实 HTTP 服务
 *
 * <p>功能说明：
 * <ul>
 *   <li>模拟设备状态数据的定时生成</li>
 *   <li>完整实现 CheckpointedFunction，支持状态保存与恢复</li>
 *   <li>支持并行执行，每个实例生成不同设备的数据</li>
 *   <li>故障恢复后从上次 Checkpoint 的偏移量继续</li>
 * </ul>
 *
 * <p>生成的数据格式（JSON）：
 * <pre>
 * {"deviceId":"device-0-5","temperature":36.7,"timestamp":"2024-01-01T12:00:00Z","offset":5}
 * </pre>
 */
public class SimulatedHttpPollingSource extends RichParallelSourceFunction<String>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatedHttpPollingSource.class);

    /** 轮询间隔（毫秒） */
    private final long intervalMs;

    /** 控制 Source 运行状态 */
    private volatile boolean running = true;

    /** 当前偏移量（已生成的记录数） */
    private long currentOffset = 0;

    /** Checkpoint 状态存储 */
    private transient ListState<Long> offsetState;

    /** 随机数生成器（模拟温度数据） */
    private transient Random random;

    /**
     * @param intervalMs 数据生成间隔（毫秒）
     */
    public SimulatedHttpPollingSource(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>("simulated-offset", Long.class);
        offsetState = context.getOperatorStateStore().getListState(descriptor);

        // 从 Checkpoint 恢复偏移量
        if (context.isRestored()) {
            for (Long offset : offsetState.get()) {
                currentOffset = offset;
            }
            LOG.info("Subtask {} restored offset to {}",
                    getRuntimeContext().getIndexOfThisSubtask(), currentOffset);
        }

        random = new Random(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(currentOffset);
        LOG.info("Subtask {} snapshot offset {} at checkpoint {}",
                getRuntimeContext().getIndexOfThisSubtask(),
                currentOffset,
                context.getCheckpointId());
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numParallel = getRuntimeContext().getNumberOfParallelSubtasks();

        LOG.info("SimulatedSource subtask {}/{} started from offset {}",
                subtaskIndex, numParallel, currentOffset);

        while (running) {
            // 模拟设备状态数据
            double temperature = 20.0 + random.nextDouble() * 30.0;
            String json = String.format(
                    "{\"deviceId\":\"device-%d-%d\",\"temperature\":%.1f,"
                            + "\"timestamp\":\"%s\",\"offset\":%d}",
                    subtaskIndex, currentOffset,
                    temperature,
                    Instant.now().toString(),
                    currentOffset
            );

            // 加锁保证数据发送和偏移量更新的原子性
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(json);
                currentOffset++;
            }

            // 等待下一次轮询
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                if (!running) {
                    break;
                }
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
