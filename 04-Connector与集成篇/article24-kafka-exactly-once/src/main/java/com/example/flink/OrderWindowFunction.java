package com.example.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 订单窗口处理函数
 * 将聚合结果转换为 OrderStats 对象
 */
public class OrderWindowFunction 
    extends ProcessWindowFunction<OrderAggregateFunction.OrderAccumulator, OrderStats, String, TimeWindow> {

    @Override
    public void process(String userId,
                       Context context,
                       Iterable<OrderAggregateFunction.OrderAccumulator> elements,
                       Collector<OrderStats> out) {
        
        OrderAggregateFunction.OrderAccumulator accumulator = elements.iterator().next();
        
        TimeWindow window = context.window();
        long windowStart = window.getStart();
        long windowEnd = window.getEnd();
        
        long orderCount = accumulator.count;
        double totalAmount = accumulator.totalAmount;
        double avgAmount = orderCount > 0 ? totalAmount / orderCount : 0.0;
        
        OrderStats stats = new OrderStats(
            userId,
            windowStart,
            windowEnd,
            orderCount,
            totalAmount,
            avgAmount
        );
        
        out.collect(stats);
    }
}
