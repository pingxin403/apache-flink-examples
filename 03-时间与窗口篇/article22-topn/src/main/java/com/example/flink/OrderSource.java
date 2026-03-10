package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 模拟订单数据源
 * 生成随机的订单数据用于测试
 */
public class OrderSource implements SourceFunction<Order> {

    private volatile boolean running = true;
    private final Random random = new Random();
    
    // 模拟20个商品
    private static final String[] PRODUCT_IDS = {
        "P001", "P002", "P003", "P004", "P005",
        "P006", "P007", "P008", "P009", "P010",
        "P011", "P012", "P013", "P014", "P015",
        "P016", "P017", "P018", "P019", "P020"
    };

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        int orderCount = 0;
        
        while (running) {
            // 生成随机订单
            String orderId = "O" + String.format("%06d", ++orderCount);
            String productId = PRODUCT_IDS[random.nextInt(PRODUCT_IDS.length)];
            String productName = "商品-" + productId;
            
            // 热门商品(P001-P005)的订单量更大
            int quantity;
            if (productId.compareTo("P005") <= 0) {
                quantity = random.nextInt(10) + 1;  // 1-10件
            } else {
                quantity = random.nextInt(5) + 1;   // 1-5件
            }
            
            long timestamp = System.currentTimeMillis();
            
            Order order = new Order(orderId, productId, productName, quantity, timestamp);
            ctx.collect(order);
            
            // 控制生成速度:每秒约100个订单
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
