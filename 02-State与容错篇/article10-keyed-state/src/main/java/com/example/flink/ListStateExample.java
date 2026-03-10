package com.example.flink;

import com.example.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * ListState 示例：记录用户最近购买记录
 * 
 * 功能：保存每个用户最近10次购买记录，分析购买偏好
 */
public class ListStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟订单流
        DataStream<Order> orderStream = env
                .addSource(new OrderSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forMonotonousTimestamps()
                                .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
                );

        // 按用户ID分组，追踪购买记录
        DataStream<String> preferences = orderStream
                .keyBy(Order::getUserId)
                .process(new RecentPurchaseTracker());

        preferences.print();

        env.execute("ListState Example - Recent Purchase Tracker");
    }

    /**
     * 最近购买追踪器
     * 使用 ListState 记录用户最近10次购买
     */
    public static class RecentPurchaseTracker extends KeyedProcessFunction<String, Order, String> {

        // 状态：记录最近购买列表
        private ListState<Order> recentOrdersState;

        private static final int MAX_HISTORY = 10;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Order> descriptor =
                    new ListStateDescriptor<>("recentOrders", Order.class);
            recentOrdersState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out)
                throws Exception {
            // 读取当前列表
            List<Order> orders = new ArrayList<>();
            for (Order o : recentOrdersState.get()) {
                orders.add(o);
            }

            // 添加新订单
            orders.add(order);

            // 保持最近10条
            if (orders.size() > MAX_HISTORY) {
                orders.remove(0);  // 移除最旧的
            }

            // 更新状态
            recentOrdersState.clear();
            recentOrdersState.addAll(orders);

            // 分析购买偏好
            String preference = analyzePreference(order.getUserId(), orders);
            out.collect(preference);
        }

        /**
         * 分析用户购买偏好
         */
        private String analyzePreference(String userId, List<Order> orders) {
            // 统计品类分布
            Map<String, Integer> categoryCount = new HashMap<>();
            double totalAmount = 0.0;

            for (Order order : orders) {
                categoryCount.merge(order.getCategory(), 1, Integer::sum);
                totalAmount += order.getAmount();
            }

            // 找出最喜欢的品类
            String favoriteCategory = categoryCount.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse("未知");

            double avgAmount = totalAmount / orders.size();

            return String.format(
                    "📊 用户 %s 购买分析：最近%d次购买，偏好品类=%s，平均消费=%.2f元",
                    userId, orders.size(), favoriteCategory, avgAmount
            );
        }
    }

    /**
     * 模拟订单源
     */
    public static class OrderSource implements SourceFunction<Order> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"user1", "user2", "user3"};
        private final String[] products = {"prod1", "prod2", "prod3", "prod4"};
        private final String[] categories = {"电子产品", "图书", "服装", "食品"};
        private final String[] cities = {"北京", "上海", "广州", "深圳"};
        private int orderIdCounter = 1;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while (running) {
                String orderId = "order" + orderIdCounter++;
                String userId = users[random.nextInt(users.length)];
                String productId = products[random.nextInt(products.length)];
                String category = categories[random.nextInt(categories.length)];
                String city = cities[random.nextInt(cities.length)];
                int quantity = random.nextInt(5) + 1;
                double amount = (random.nextInt(500) + 50) * quantity;
                long timestamp = System.currentTimeMillis();

                Order order = new Order(orderId, userId, productId, category,
                        city, quantity, amount, timestamp);
                ctx.collect(order);

                Thread.sleep(1000);  // 每秒一个订单
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
