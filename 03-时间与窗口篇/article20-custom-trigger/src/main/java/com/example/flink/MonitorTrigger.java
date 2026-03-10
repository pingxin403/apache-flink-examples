package com.example.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 监控触发器:正常每 5 分钟触发,但 QPS 或错误率超阈值时立即触发
 */
public class MonitorTrigger extends Trigger<MetricEvent, TimeWindow> {
    
    private final long qpsThreshold = 10000;
    private final double errorRateThreshold = 0.05;
    
    private final ValueStateDescriptor<Long> qpsStateDesc = 
        new ValueStateDescriptor<>("qps", Long.class);
    private final ValueStateDescriptor<Double> errorRateStateDesc = 
        new ValueStateDescriptor<>("errorRate", Double.class);
    
    @Override
    public TriggerResult onElement(MetricEvent element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) throws Exception {
        // 注册窗口结束定时器
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        // 更新 QPS 状态
        ValueState<Long> qpsState = ctx.getPartitionedState(qpsStateDesc);
        long currentQps = qpsState.value() == null ? 0 : qpsState.value();
        qpsState.update(currentQps + element.getQps());
        
        // 更新错误率状态
        ValueState<Double> errorRateState = ctx.getPartitionedState(errorRateStateDesc);
        double currentErrorRate = element.getErrorRate();
        errorRateState.update(currentErrorRate);
        
        // 检查是否需要立即触发
        if (currentQps + element.getQps() > qpsThreshold || 
            currentErrorRate > errorRateThreshold) {
            return TriggerResult.FIRE;  // 立即触发但不清理
        }
        
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, 
                                     TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE;
    }
    
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, 
                                         TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }
    

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.getPartitionedState(qpsStateDesc).clear();
        ctx.getPartitionedState(errorRateStateDesc).clear();
    }
}
