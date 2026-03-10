package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 订单数据源 - 模拟生成订单数据
 */
public class OrderSource implements SourceFunction<Order> {
    private volatile boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        int orderCount = 0;
        
        while (running && orderCount < 100) {
            // 生成订单数据
            String orderId = "order_" + orderCount;
            String userId = "user_" + random.nextInt(10);  // 10个用户
            Double amount = 50.0 + random.nextDouble() * 450.0;  // 50-500元
            Long timestamp = System.currentTimeMillis();
            
            Order order = new Order(orderId, userId, amount, timestamp);
            ctx.collect(order);
            
            orderCount++;
            
            // 模拟订单到达间隔：100-500ms
            Thread.sleep(100 + random.nextInt(400));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
