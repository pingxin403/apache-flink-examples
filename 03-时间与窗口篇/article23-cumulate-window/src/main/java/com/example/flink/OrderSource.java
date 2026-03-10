package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

/**
 * 订单数据源:模拟生成订单数据
 */
public class OrderSource implements SourceFunction<Order> {
    
    private volatile boolean running = true;
    private final Random random = new Random();
    
    // 模拟的用户 ID 列表
    private final String[] userIds = {
        "user_001", "user_002", "user_003", "user_004", "user_005",
        "user_006", "user_007", "user_008", "user_009", "user_010"
    };
    
    // 模拟的商品 ID 列表
    private final String[] productIds = {
        "prod_001", "prod_002", "prod_003", "prod_004", "prod_005",
        "prod_006", "prod_007", "prod_008", "prod_009", "prod_010"
    };

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running) {
            // 生成随机订单
            String orderId = UUID.randomUUID().toString();
            String userId = userIds[random.nextInt(userIds.length)];
            String productId = productIds[random.nextInt(productIds.length)];
            
            // 生成 10-1000 之间的随机金额
            BigDecimal amount = BigDecimal.valueOf(10 + random.nextInt(990));
            
            // 使用当前时间作为订单时间
            LocalDateTime orderTime = LocalDateTime.now();
            
            Order order = new Order(orderId, userId, productId, amount, orderTime);
            
            // 发送订单数据
            ctx.collect(order);
            
            // 每秒生成 10 个订单
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
