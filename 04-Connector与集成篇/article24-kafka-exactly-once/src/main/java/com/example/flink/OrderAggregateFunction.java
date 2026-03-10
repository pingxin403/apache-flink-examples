package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 订单聚合函数
 * 使用增量聚合,高效计算订单统计
 */
public class OrderAggregateFunction implements AggregateFunction<Order, OrderAccumulator, OrderAccumulator> {

    @Override
    public OrderAccumulator createAccumulator() {
        return new OrderAccumulator();
    }

    @Override
    public OrderAccumulator add(Order order, OrderAccumulator accumulator) {
        accumulator.count++;
        accumulator.totalAmount += order.getAmount();
        return accumulator;
    }

    @Override
    public OrderAccumulator getResult(OrderAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
        a.count += b.count;
        a.totalAmount += b.totalAmount;
        return a;
    }

    /**
     * 订单累加器
     */
    public static class OrderAccumulator {
        public long count = 0;
        public double totalAmount = 0.0;
    }
}
