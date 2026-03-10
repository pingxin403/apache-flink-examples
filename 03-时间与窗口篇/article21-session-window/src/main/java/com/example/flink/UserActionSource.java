package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 用户行为数据源
 * 模拟生成用户浏览、加购、下单等行为数据
 */
public class UserActionSource implements SourceFunction<UserAction> {
    
    private volatile boolean running = true;
    private final Random random = new Random();
    private final String[] users = {"user1", "user2", "user3", "user4", "user5"};
    private final String[] products = {"product1", "product2", "product3", "product4", "product5"};
    private final String[] actionTypes = {"VIEW", "VIEW", "VIEW", "ADD_CART", "ORDER"};
    
    @Override
    public void run(SourceContext<UserAction> ctx) throws Exception {
        while (running) {
            // 随机选择用户
            String userId = users[random.nextInt(users.length)];
            
            // 随机选择行为类型(浏览的概率更高)
            String actionType = actionTypes[random.nextInt(actionTypes.length)];
            
            // 随机选择商品
            String productId = products[random.nextInt(products.length)];
            
            // 随机生成价格
            Double price = 100.0 + random.nextDouble() * 900.0;
            
            // 使用当前时间作为事件时间
            Long timestamp = System.currentTimeMillis();
            
            UserAction action = new UserAction(userId, actionType, productId, price, timestamp);
            ctx.collect(action);
            
            // 随机休眠 1-5 秒,模拟用户行为间隔
            Thread.sleep(1000 + random.nextInt(4000));
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
