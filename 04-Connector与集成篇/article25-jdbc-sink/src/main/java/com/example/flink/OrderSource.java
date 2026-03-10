package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 订单数据源 - 模拟生成订单数据
 */
public class OrderSource implements SourceFunction<Order> {
    private volatile boolean running = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running) {
            // 生成随机订单数据
            String orderId = UUID.randomUUID().toString();
            String userId = "user-" + random.nextInt(100);
            Double amount = 10.0 + random.nextDouble() * 990.0;  // 10-1000 之间的金额

            Order order = new Order(orderId, userId, amount);
            ctx.collect(order);

            // 控制生成速率:每秒约 100 条
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
