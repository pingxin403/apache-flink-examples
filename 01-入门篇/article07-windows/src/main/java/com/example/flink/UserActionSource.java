package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 用户行为数据源 - 模拟生成用户行为数据
 * 
 * 模拟场景：
 * - 用户会有活跃期和不活跃期
 * - 活跃期内频繁操作（间隔短）
 * - 不活跃期间隔较长（超过Session Gap）
 */
public class UserActionSource implements SourceFunction<UserAction> {
    private volatile boolean running = true;
    private Random random = new Random();
    
    private static final String[] ACTIONS = {"click", "view", "purchase", "add_to_cart"};
    private static final String[] PAGES = {"home", "product", "cart", "checkout", "profile"};

    @Override
    public void run(SourceContext<UserAction> ctx) throws Exception {
        int actionCount = 0;
        
        while (running && actionCount < 50) {
            // 随机选择用户
            String userId = "user_" + random.nextInt(5);
            
            // 模拟一个用户的活跃期：连续产生3-8个行为
            int sessionSize = 3 + random.nextInt(6);
            
            for (int i = 0; i < sessionSize && running; i++) {
                String action = ACTIONS[random.nextInt(ACTIONS.length)];
                String page = PAGES[random.nextInt(PAGES.length)];
                Long timestamp = System.currentTimeMillis();
                
                UserAction userAction = new UserAction(userId, action, page, timestamp);
                ctx.collect(userAction);
                
                actionCount++;
                
                // 活跃期内的行为间隔较短：1-3秒
                Thread.sleep(1000 + random.nextInt(2000));
            }
            
            // 模拟不活跃期：间隔较长，超过Session Gap（这里设置为5-10秒）
            Thread.sleep(5000 + random.nextInt(5000));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
