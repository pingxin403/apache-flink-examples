package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Random;

/**
 * 订单数据源（模拟包含迟到数据的场景）
 * 
 * 数据生成策略：
 * 1. 正常数据：90% 的数据按时到达
 * 2. 轻微延迟数据：8% 的数据延迟 5-15 秒（在 Watermark 容忍范围内）
 * 3. 严重延迟数据：2% 的数据延迟 20-50 秒（超过 Watermark + Allowed Lateness）
 */
public class OrderSourceWithLateData implements SourceFunction<Order> {

    private volatile boolean running = true;
    private final Random random = new Random();
    private long baseTimestamp;
    private int orderCounter = 0;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        // 设置基准时间为当前时间
        baseTimestamp = System.currentTimeMillis();

        while (running) {
            Order order = generateOrder();
            ctx.collect(order);

            // 控制数据生成速度：每秒生成 10 条订单
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * 生成订单数据
     * 90% 正常数据，8% 轻微延迟，2% 严重延迟
     */
    private Order generateOrder() {
        orderCounter++;
        String orderId = "ORDER_" + String.format("%05d", orderCounter);
        String userId = "USER_" + random.nextInt(100);
        BigDecimal amount = BigDecimal.valueOf(random.nextInt(1000) + 100);

        // 计算事件时间戳
        long eventTimestamp;
        double rand = random.nextDouble();

        if (rand < 0.90) {
            // 90% 正常数据：事件时间 = 当前时间
            eventTimestamp = System.currentTimeMillis();
        } else if (rand < 0.98) {
            // 8% 轻微延迟数据：延迟 5-15 秒
            int delay = 5000 + random.nextInt(10000);
            eventTimestamp = System.currentTimeMillis() - delay;
        } else {
            // 2% 严重延迟数据：延迟 20-50 秒
            int delay = 20000 + random.nextInt(30000);
            eventTimestamp = System.currentTimeMillis() - delay;
        }

        return new Order(orderId, userId, amount, eventTimestamp);
    }
}
