package com.example.flink;

import com.example.flink.model.Alert;
import com.example.flink.model.ConfigRule;
import com.example.flink.model.DataEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Random;

/**
 * BroadcastState 完整示例
 * 
 * 功能：
 * 1. 配置流：动态更新规则
 * 2. 数据流：根据最新规则过滤数据
 * 3. 规则随 Checkpoint 持久化
 * 
 * 运行方式：
 * mvn exec:java -Dexec.mainClass="com.example.flink.BroadcastStateExample"
 */
public class BroadcastStateExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 启用 Checkpoint（每 10 秒）
        env.enableCheckpointing(10000);
        
        // 设置并行度
        env.setParallelism(2);
        
        // 数据流：生成数据事件
        DataStream<DataEvent> dataStream = env
            .addSource(new DataEventSource())
            .name("data-event-source");
        
        // 配置流：生成配置规则
        DataStream<ConfigRule> configStream = env
            .addSource(new ConfigRuleSource())
            .name("config-rule-source")
            .setParallelism(1);  // 配置流通常单并行度
        
        // 定义 BroadcastState 描述符
        MapStateDescriptor<String, ConfigRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                "config-rules",
                String.class,
                ConfigRule.class
            );
        
        // 广播配置流
        BroadcastStream<ConfigRule> broadcastRules = 
            configStream.broadcast(ruleStateDescriptor);
        
        // 连接数据流和广播流
        DataStream<Alert> alerts = dataStream
            .keyBy(DataEvent::getUserId)
            .connect(broadcastRules)
            .process(new RuleBasedProcessor(ruleStateDescriptor))
            .name("rule-based-processor");
        
        // 输出结果
        alerts.print();
        
        env.execute("BroadcastState Example");
    }
    
    /**
     * 数据事件源
     * 每秒生成随机数据事件
     */
    public static class DataEventSource extends RichParallelSourceFunction<DataEvent> {
        private volatile boolean isRunning = true;
        private Random random = new Random();
        
        @Override
        public void run(SourceContext<DataEvent> ctx) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            
            while (isRunning) {
                String userId = "user-" + (subtaskIndex * 10 + random.nextInt(10));
                double value = random.nextDouble() * 200;
                long timestamp = System.currentTimeMillis();
                
                DataEvent event = new DataEvent(userId, value, timestamp);
                ctx.collect(event);
                
                Thread.sleep(1000);
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    
    /**
     * 配置规则源
     * 每 10 秒更新一次规则
     */
    public static class ConfigRuleSource extends RichParallelSourceFunction<ConfigRule> {
        private volatile boolean isRunning = true;
        private Random random = new Random();
        
        @Override
        public void run(SourceContext<ConfigRule> ctx) throws Exception {
            // 初始规则
            ctx.collect(new ConfigRule("rule-1", "threshold", 100.0));
            
            int count = 0;
            while (isRunning) {
                Thread.sleep(10000);
                
                // 动态更新规则
                count++;
                double newThreshold = 100.0 + (count % 3) * 50.0;
                ConfigRule rule = new ConfigRule("rule-1", "threshold", newThreshold);
                
                System.out.println("📢 规则更新：" + rule);
                ctx.collect(rule);
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    
    /**
     * 基于规则的处理函数
     * 使用 BroadcastState 存储规则
     */
    public static class RuleBasedProcessor extends KeyedBroadcastProcessFunction<
            String,           // Key 类型（userId）
            DataEvent,        // 数据流类型
            ConfigRule,       // 广播流类型
            Alert> {          // 输出类型
        
        private final MapStateDescriptor<String, ConfigRule> ruleStateDescriptor;
        
        public RuleBasedProcessor(MapStateDescriptor<String, ConfigRule> ruleStateDescriptor) {
            this.ruleStateDescriptor = ruleStateDescriptor;
        }
        
        /**
         * 处理数据流（只读广播状态）
         */
        @Override
        public void processElement(
                DataEvent event,
                ReadOnlyContext ctx,
                Collector<Alert> out) throws Exception {
            
            // 读取广播状态（只读）
            ReadOnlyBroadcastState<String, ConfigRule> ruleState = 
                ctx.getBroadcastState(ruleStateDescriptor);
            
            // 遍历所有规则，检测数据
            boolean passed = true;
            for (Map.Entry<String, ConfigRule> entry : ruleState.immutableEntries()) {
                ConfigRule rule = entry.getValue();
                
                if (rule.getRuleType().equals("threshold")) {
                    // 阈值规则
                    if (event.getValue() < rule.getThreshold()) {
                        passed = false;
                        out.collect(new Alert(
                            event.getUserId(),
                            String.format("❌ 数据被过滤：value=%.2f < threshold=%.2f",
                                event.getValue(), rule.getThreshold())
                        ));
                    }
                }
            }
            
            if (passed) {
                out.collect(new Alert(
                    event.getUserId(),
                    String.format("✅ 数据通过检查：value=%.2f", event.getValue())
                ));
            }
        }
        
        /**
         * 处理广播流（可写广播状态）
         */
        @Override
        public void processBroadcastElement(
                ConfigRule rule,
                Context ctx,
                Collector<Alert> out) throws Exception {
            
            // 更新广播状态（可写）
            BroadcastState<String, ConfigRule> ruleState = 
                ctx.getBroadcastState(ruleStateDescriptor);
            
            // 添加或更新规则
            ruleState.put(rule.getRuleId(), rule);
            
            System.out.println("✨ 规则已应用到实例 " + 
                getRuntimeContext().getIndexOfThisSubtask() + ": " + rule);
        }
    }
}
