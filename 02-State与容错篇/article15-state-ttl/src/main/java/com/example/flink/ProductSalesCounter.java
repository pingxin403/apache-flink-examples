package com.example.flink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 商品销量统计示例
 * 演示如何使用 TTL 统计商品最近 7 天的销量
 */
public class ProductSalesCounter extends KeyedProcessFunction<String, Order, ProductSales> {
    
    private ValueState<Long> salesCountState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 配置 TTL：7 天
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.days(7))
            // 只在写入时重置 TTL（不关心读取）
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            // 过期状态永不返回
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            // 启用 RocksDB 压缩时清理（后台清理，不影响前台性能）
            .cleanupInRocksdbCompactFilter(1000)
            .build();
        
        ValueStateDescriptor<Long> descriptor = 
            new ValueStateDescriptor<>("salesCount", Long.class);
        descriptor.enableTimeToLive(ttlConfig);
        
        salesCountState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(Order order, Context ctx, Collector<ProductSales> out) 
            throws Exception {
        Long currentCount = salesCountState.value();
        if (currentCount == null) {
            currentCount = 0L;
        }
        
        // 累加销量
        currentCount += order.getQuantity();
        salesCountState.update(currentCount);
        
        // 输出统计结果
        out.collect(new ProductSales(
            order.getProductId(),
            currentCount,
            "最近 7 天销量"
        ));
    }
}
