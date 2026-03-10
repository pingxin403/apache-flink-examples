package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 用户行为数据源（模拟）
 * 
 * 生成模拟的用户行为数据，用于测试 Elasticsearch Sink
 */
public class UserBehaviorSource implements SourceFunction<UserBehavior> {
    
    private volatile boolean running = true;
    private Random random = new Random();
    
    // 模拟数据
    private static final String[] ACTIONS = {"view", "click", "cart", "purchase"};
    private static final String[] CATEGORIES = {"电子产品", "服装", "食品", "图书", "家居"};
    
    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        while (running) {
            // 生成随机用户行为数据
            UserBehavior behavior = new UserBehavior(
                "user_" + random.nextInt(1000),                    // 用户ID
                ACTIONS[random.nextInt(ACTIONS.length)],           // 行为类型
                "product_" + random.nextInt(10000),                // 商品ID
                CATEGORIES[random.nextInt(CATEGORIES.length)],     // 商品类目
                System.currentTimeMillis()                         // 事件时间
            );
            
            ctx.collect(behavior);
            
            // 控制数据生成速率：每秒约 100 条
            Thread.sleep(10);
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
