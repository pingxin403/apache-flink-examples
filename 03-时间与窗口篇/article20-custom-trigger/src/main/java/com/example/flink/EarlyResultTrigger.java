package com.example.flink;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 周期性提前触发器:在窗口结束前按固定间隔触发
 */
public class EarlyResultTrigger<T> extends Trigger<T, TimeWindow> {
    
    private final long interval;  // 提前触发的间隔(毫秒)
    
    public EarlyResultTrigger(long interval) {
        this.interval = interval;
    }
    
    @Override
    public TriggerResult onElement(T element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) {
        // 注册窗口结束时间的定时器
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        // 注册第一个提前触发的定时器
        long firstFireTime = window.getStart() + interval;
        if (firstFireTime < window.maxTimestamp()) {
            ctx.registerEventTimeTimer(firstFireTime);
        }
        
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, 
                                     TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            // 窗口结束,触发并清理
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // 提前触发,注册下一个定时器
            long nextFireTime = time + interval;
            if (nextFireTime < window.maxTimestamp()) {
                ctx.registerEventTimeTimer(nextFireTime);
            }
            return TriggerResult.FIRE;  // 触发但不清理
        }
    }
    
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, 
                                         TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
