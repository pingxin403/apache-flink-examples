package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 自定义触发器示例
 * 演示如何使用自定义触发器实现灵活的窗口触发逻辑
 */
public class CustomTriggerExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建测试数据源
        DataStream<MetricEvent> metrics = env.fromElements(
            new MetricEvent("service-a", 1000L, 100, 0.01, 50.0),
            new MetricEvent("service-a", 2000L, 200, 0.02, 60.0),
            new MetricEvent("service-a", 3000L, 300, 0.03, 150.0),  // 超过阈值,会触发
            new MetricEvent("service-a", 4000L, 400, 0.04, 70.0),
            new MetricEvent("service-a", 5000L, 500, 0.05, 80.0),
            new MetricEvent("service-b", 1000L, 150, 0.01, 55.0),
            new MetricEvent("service-b", 2000L, 250, 0.02, 65.0),
            new MetricEvent("service-b", 3000L, 350, 0.08, 75.0),  // 错误率超阈值,会触发
            new MetricEvent("service-b", 4000L, 450, 0.04, 85.0),
            new MetricEvent("service-b", 5000L, 550, 0.05, 95.0)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<MetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );
        
        // 示例1:使用阈值触发器
        System.out.println("=== 示例1:阈值触发器 ===");
        metrics
            .keyBy(MetricEvent::getServiceName)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .trigger(new ThresholdTrigger(100.0))  // 值超过 100 时触发
            .aggregate(new MetricAggregateFunction())
            .print("阈值触发");
        
        // 示例2:使用计数或时间触发器
        System.out.println("\n=== 示例2:计数或时间触发器 ===");
        metrics
            .keyBy(MetricEvent::getServiceName)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .trigger(new CountOrTimeTrigger<>(3))  // 3 条数据或窗口结束时触发
            .aggregate(new MetricAggregateFunction())
            .print("计数触发");
        
        // 示例3:使用周期性提前触发器
        System.out.println("\n=== 示例3:周期性提前触发器 ===");
        metrics
            .keyBy(MetricEvent::getServiceName)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .trigger(new EarlyResultTrigger<>(2000))  // 每 2 秒触发一次
            .aggregate(new MetricAggregateFunction())
            .print("周期触发");
        
        // 示例4:使用监控触发器
        System.out.println("\n=== 示例4:监控触发器 ===");
        metrics
            .keyBy(MetricEvent::getServiceName)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .trigger(new MonitorTrigger())  // QPS 或错误率超阈值时触发
            .aggregate(new MetricAggregateFunction())
            .print("监控触发");
        
        env.execute("Custom Trigger Example");
    }
    

    /**
     * 指标聚合函数
     */
    private static class MetricAggregateFunction 
            implements AggregateFunction<MetricEvent, MetricAccumulator, MetricResult> {
        
        @Override
        public MetricAccumulator createAccumulator() {
            return new MetricAccumulator();
        }
        
        @Override
        public MetricAccumulator add(MetricEvent value, MetricAccumulator accumulator) {
            accumulator.count++;
            accumulator.totalQps += value.getQps();
            accumulator.maxErrorRate = Math.max(accumulator.maxErrorRate, value.getErrorRate());
            accumulator.totalValue += value.getValue();
            return accumulator;
        }
        
        @Override
        public MetricResult getResult(MetricAccumulator accumulator) {
            return new MetricResult(
                accumulator.count,
                accumulator.totalQps,
                accumulator.maxErrorRate,
                accumulator.totalValue / accumulator.count
            );
        }
        
        @Override
        public MetricAccumulator merge(MetricAccumulator a, MetricAccumulator b) {
            a.count += b.count;
            a.totalQps += b.totalQps;
            a.maxErrorRate = Math.max(a.maxErrorRate, b.maxErrorRate);
            a.totalValue += b.totalValue;
            return a;
        }
    }
    
    /**
     * 累加器
     */
    private static class MetricAccumulator {
        long count = 0;
        long totalQps = 0;
        double maxErrorRate = 0.0;
        double totalValue = 0.0;
    }
    
    /**
     * 聚合结果
     */
    private static class MetricResult {
        long count;
        long totalQps;
        double maxErrorRate;
        double avgValue;
        
        public MetricResult(long count, long totalQps, double maxErrorRate, double avgValue) {
            this.count = count;
            this.totalQps = totalQps;
            this.maxErrorRate = maxErrorRate;
            this.avgValue = avgValue;
        }
        
        @Override
        public String toString() {
            return String.format("MetricResult{count=%d, totalQps=%d, maxErrorRate=%.2f%%, avgValue=%.2f}",
                count, totalQps, maxErrorRate * 100, avgValue);
        }
    }
}
