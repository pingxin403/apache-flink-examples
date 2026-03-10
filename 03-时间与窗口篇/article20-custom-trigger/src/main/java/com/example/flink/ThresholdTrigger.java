package com.example.flink;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 阈值触发器:当指标超过阈值时立即触发,否则等到窗口结束
 */
public class ThresholdTrigger extends Trigger<MetricEvent, TimeWindow> {
    
    private final double threshold;
    
    public ThresholdTrigger(double threshold) {
        this.threshold = threshold;
    }
    
    @Override
    public TriggerResult onElement(MetricEvent element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) {
        // 注册窗口结束时间的定时器
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        // 如果指标超过阈值,立即触发
        if (element.getValue() > threshold) {
            return TriggerResult.FIRE;  // 触发计算但不清理窗口
        }
        
        return TriggerResult.CONTINUE;  // 继续等待
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        // 窗口结束时触发并清理
        return TriggerResult.FIRE_AND_PURGE;
    }
    
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
