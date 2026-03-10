package com.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 设备异常检测示例
 * 演示如何使用 TTL 管理设备读数，只保留最近 1 小时的数据
 */
public class DeviceAnomalyDetector extends KeyedProcessFunction<String, SensorReading, Alert> {
    
    private ListState<SensorReading> readingsState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 配置 TTL：1 小时
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(1))
            // 只在写入时重置 TTL
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            // 过期状态永不返回
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            // 启用增量清理
            .cleanupIncrementally(10, true)
            .build();
        
        ListStateDescriptor<SensorReading> descriptor = 
            new ListStateDescriptor<>("readings", SensorReading.class);
        descriptor.enableTimeToLive(ttlConfig);
        
        readingsState = getRuntimeContext().getListState(descriptor);
    }
    
    @Override
    public void processElement(SensorReading reading, Context ctx, Collector<Alert> out) 
            throws Exception {
        // 添加新读数
        readingsState.add(reading);
        
        // 获取最近 1 小时的所有读数
        List<SensorReading> recentReadings = new ArrayList<>();
        for (SensorReading r : readingsState.get()) {
            recentReadings.add(r);
        }
        
        // 检测异常
        if (detectAnomaly(recentReadings)) {
            out.collect(new Alert(
                reading.getDeviceId(),
                "检测到异常读数: " + reading.getValue(),
                System.currentTimeMillis()
            ));
        }
    }
    
    /**
     * 简单的异常检测逻辑：
     * 如果最新读数偏离平均值超过 50%，则认为异常
     */
    private boolean detectAnomaly(List<SensorReading> readings) {
        if (readings.size() < 10) {
            return false;  // 数据不足，无法判断
        }
        
        // 计算平均值
        double avg = readings.stream()
            .mapToDouble(SensorReading::getValue)
            .average()
            .orElse(0.0);
        
        // 获取最新读数
        double lastValue = readings.get(readings.size() - 1).getValue();
        
        // 判断是否偏离平均值超过 50%
        return Math.abs(lastValue - avg) > avg * 0.5;
    }
}
