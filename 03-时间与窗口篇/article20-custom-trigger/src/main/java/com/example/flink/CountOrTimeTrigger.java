package com.example.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 计数或时间触发器:数据量达到阈值或窗口结束时触发
 */
public class CountOrTimeTrigger<T> extends Trigger<T, TimeWindow> {
    
    private final long maxCount;
    private final ReducingStateDescriptor<Long> countStateDesc;
    
    public CountOrTimeTrigger(long maxCount) {
        this.maxCount = maxCount;
        this.countStateDesc = new ReducingStateDescriptor<>(
            "count", new Sum(), Long.class);
    }
    
    @Override
    public TriggerResult onElement(T element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) throws Exception {
        // 注册窗口结束时间的定时器
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        // 累加计数
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        count.add(1L);
        
        // 如果数量达到阈值,触发计算但不清理窗口
        if (count.get() >= maxCount) {
            count.clear();  // 清理计数状态
            return TriggerResult.FIRE;
        }
        
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, 
                                     TriggerContext ctx) throws Exception {
        // 窗口结束时触发并清理
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
        ctx.getPartitionedState(countStateDesc).clear();
    }
    

    /**
     * 求和函数
     */
    private static class Sum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }
}
