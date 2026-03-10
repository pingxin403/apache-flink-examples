package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 销量聚合函数
 * 用于增量聚合每个商品在窗口内的销量
 */
public class SalesAggregateFunction implements AggregateFunction<Order, Long, Long> {

    /**
     * 创建累加器,初始值为0
     */
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    /**
     * 将新订单的数量累加到累加器
     */
    @Override
    public Long add(Order order, Long accumulator) {
        return accumulator + order.getQuantity();
    }

    /**
     * 获取聚合结果
     */
    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    /**
     * 合并两个累加器(用于会话窗口或状态合并)
     */
    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
