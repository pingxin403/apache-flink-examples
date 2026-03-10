package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

/**
 * 订单数据源,模拟生成订单数据
 */
public class OrderSource implements SourceFunction<Order> {
    private volatile boolean running = true;
    private final Random random = new Random();
    private final String[] products = {"PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005"};

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running) {
            // 生成订单数据
            String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);
            String userId = "USER-" + random.nextInt(100);
            String productId = products[random.nextInt(products.length)];
            BigDecimal amount = BigDecimal.valueOf(10 + random.nextInt(990));
            long timestamp = System.currentTimeMillis();

            Order order = new Order(orderId, userId, productId, amount, timestamp);
            ctx.collect(order);

            // 控制数据生成速率
            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
