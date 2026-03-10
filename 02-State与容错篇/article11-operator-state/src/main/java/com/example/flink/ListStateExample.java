package com.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ListState 完整示例
 * 
 * 功能：
 * 1. 带缓冲的 Source，每 10 条数据发送一次
 * 2. Checkpoint 时保存缓冲区
 * 3. 故障恢复时恢复缓冲区
 * 4. 支持并行度变化时的状态重分配
 * 
 * 运行方式：
 * mvn exec:java -Dexec.mainClass="com.example.flink.ListStateExample"
 */
public class ListStateExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 启用 Checkpoint（每 5 秒）
        env.enableCheckpointing(5000);
        
        // 设置并行度
        env.setParallelism(2);
        
        // 带缓冲的 Source
        DataStream<String> dataStream = env
            .addSource(new BufferedSource())
            .name("buffered-source");
        
        // 输出结果
        dataStream.print();
        
        env.execute("ListState Example");
    }
    
    /**
     * 带缓冲的 Source
     * 使用 ListState 保存缓冲区
     */
    public static class BufferedSource extends RichParallelSourceFunction<String> 
            implements CheckpointedFunction {
        
        private volatile boolean isRunning = true;
        private Random random = new Random();
        
        // 运行时缓冲区
        private List<String> buffer;
        
        // Operator State：持久化缓冲区
        private transient ListState<String> checkpointedState;
        
        // 批次大小
        private static final int BATCH_SIZE = 10;
        
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int count = 0;
            
            while (isRunning) {
                // 生成数据
                String data = String.format("data-%d-%d", subtaskIndex, count++);
                
                // 添加到缓冲区
                buffer.add(data);
                System.out.println("📦 实例 " + subtaskIndex + " 缓冲区大小：" + buffer.size());
                
                // 缓冲区满了，批量发送
                if (buffer.size() >= BATCH_SIZE) {
                    synchronized (ctx.getCheckpointLock()) {
                        System.out.println("🚀 实例 " + subtaskIndex + " 发送批次：" + buffer);
                        
                        for (String item : buffer) {
                            ctx.collect(item);
                        }
                        
                        buffer.clear();
                    }
                }
                
                Thread.sleep(500);
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
        
        /**
         * Checkpoint 时保存状态
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            
            // 清空状态
            checkpointedState.clear();
            
            // 将缓冲区数据保存到状态
            for (String item : buffer) {
                checkpointedState.add(item);
            }
            
            System.out.println("💾 实例 " + subtaskIndex + 
                " Checkpoint " + context.getCheckpointId() + 
                " 保存了 " + buffer.size() + " 条数据");
        }
        
        /**
         * 初始化或恢复状态
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            
            // 定义 ListState 描述符
            ListStateDescriptor<String> descriptor = 
                new ListStateDescriptor<>("buffered-data", String.class);
            
            // 获取 ListState（Even-split 模式）
            checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
            
            // 如果使用 Union 模式，改为：
            // checkpointedState = context.getOperatorStateStore()
            //     .getUnionListState(descriptor);
            
            // 初始化缓冲区
            buffer = new ArrayList<>();
            
            // 如果是恢复，加载状态
            if (context.isRestored()) {
                for (String item : checkpointedState.get()) {
                    buffer.add(item);
                }
                
                System.out.println("🔄 实例 " + subtaskIndex + 
                    " 恢复了 " + buffer.size() + " 条数据：" + buffer);
            } else {
                System.out.println("🆕 实例 " + subtaskIndex + " 首次启动");
            }
        }
    }
    
    /**
     * 演示并行度变化时的状态重分配
     * 
     * 测试步骤：
     * 1. 并行度 2 运行，触发 Checkpoint
     * 2. 从 Savepoint 恢复，并行度改为 3
     * 3. 观察状态如何重分配
     * 
     * Even-split 模式：
     * - 实例 0 的状态：[A, B, C, D]
     * - 实例 1 的状态：[E, F, G, H]
     * - 重分配后：
     *   - 新实例 0：[A, B, C]
     *   - 新实例 1：[D, E, F]
     *   - 新实例 2：[G, H]
     * 
     * Union 模式：
     * - 实例 0 的状态：[A, B]
     * - 实例 1 的状态：[C, D]
     * - 重分配后：
     *   - 新实例 0：[A, B, C, D]
     *   - 新实例 1：[A, B, C, D]
     *   - 新实例 2：[A, B, C, D]
     */
    public static void demonstrateRescaling() {
        System.out.println("=== 并行度变化时的状态重分配 ===");
        System.out.println();
        System.out.println("Even-split 模式（默认）：");
        System.out.println("- 状态均匀分割给新实例");
        System.out.println("- 适用场景：Source 偏移量、缓冲数据");
        System.out.println();
        System.out.println("Union 模式：");
        System.out.println("- 所有新实例获得完整状态");
        System.out.println("- 适用场景：广播配置（但推荐用 BroadcastState）");
        System.out.println();
        System.out.println("测试命令：");
        System.out.println("# 1. 并行度 2 运行");
        System.out.println("flink run -p 2 target/article11-operator-state-1.0-SNAPSHOT.jar");
        System.out.println();
        System.out.println("# 2. 触发 Savepoint");
        System.out.println("flink savepoint <job-id> hdfs://path/to/savepoint");
        System.out.println();
        System.out.println("# 3. 从 Savepoint 恢复，并行度改为 3");
        System.out.println("flink run -p 3 -s hdfs://path/to/savepoint \\");
        System.out.println("    target/article11-operator-state-1.0-SNAPSHOT.jar");
    }
}
