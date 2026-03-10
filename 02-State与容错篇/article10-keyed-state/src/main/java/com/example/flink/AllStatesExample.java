package com.example.flink;

import com.example.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 综合示例：使用所有四种 Keyed State
 * 
 * 场景：用户行为分析系统
 * - ValueState: 记录用户最新订单时间
 * - ListState: 保存最近5次购买记录
 * - MapState: 统计各品类购买次数
 * - ReducingState: 累计消费总额
 */
public class AllStatesExample {

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

        // 按用户ID分组，进行综合分析
        DataStream<String> userAnalysis = orderStream
                .keyBy(Order::getUserId)
                .process(new UserBehaviorAnalyzer());

        userAnalysis.print();

        env.execute("All States Example - User Behavior Analyzer");
    }

    /**
     * 用户行为分析器
     * 综合使用四种状态类型
     */
    public static class UserBehaviorAnalyzer extends KeyedProcessFunction<String, Order, String> {

        // ValueState: 最新订单时间
        private ValueState<Long> lastOrderTimeState;

        // ListState: 最近5次购买记录
        private ListState<String> recentProductsState;

        // MapState: 各品类购买次数
        private MapState<String, Integer> categoryCountState;

        // ReducingState: 累计消费总额
        private ReducingState<Double> totalAmountState;

        private static final int MAX_RECENT_PRODUCTS = 5;

        @Override
        public void open(Configuration parameters) {
            // 初始化 ValueState
            lastOrderTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastOrderTime", Long.class)
            );

            // 初始化 ListState
            recentProductsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recentProducts", String.class)
            );

            // 初始化 MapState
            categoryCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("categoryCount", String.class, Integer.class)
            );

            // 初始化 ReducingState
            totalAmountState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<>(
                            "totalAmount",
                            new ReduceFunction<Double>() {
                                @Override
                                public Double reduce(Double v1, Double v2) {
                                    return v1 + v2;
                                }
                            },
                            Double.class
                    )
            );
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out)
                throws Exception {
            String userId = order.getUserId();

            // 1. 使用 ValueState：检查订单间隔
            Long lastOrderTime = lastOrderTimeState.value();
            long currentTime = order.getTimestamp();
            String timeGap = "";

            if (lastOrderTime != null) {
                long gap = (currentTime - lastOrderTime) / 1000 / 60;  // 分钟
                timeGap = String.format("距上次购买 %d 分钟", gap);
            } else {
                timeGap = "首次购买";
            }
            lastOrderTimeState.update(currentTime);

            // 2. 使用 ListState：维护最近购买商品列表
            List<String> recentProducts = new ArrayList<>();
            for (String product : recentProductsState.get()) {
                recentProducts.add(product);
            }
            recentProducts.add(order.getProductId());
            if (recentProducts.size() > MAX_RECENT_PRODUCTS) {
                recentProducts.remove(0);
            }
            recentProductsState.clear();
            recentProductsState.addAll(recentProducts);

            // 3. 使用 MapState：统计品类购买次数
            String category = order.getCategory();
            Integer count = categoryCountState.get(category);
            categoryCountState.put(category, (count == null ? 0 : count) + 1);

            // 找出最喜欢的品类
            String favoriteCategory = "";
            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : categoryCountState.entries()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    favoriteCategory = entry.getKey();
                }
            }

            // 4. 使用 ReducingState：累计消费总额
            totalAmountState.add(order.getAmount());
            Double totalAmount = totalAmountState.get();

            // 生成综合分析报告
            StringBuilder report = new StringBuilder();
            report.append("\n" + "=".repeat(60) + "\n");
            report.append(String.format("👤 用户 %s 行为分析报告\n", userId));
            report.append("=".repeat(60) + "\n");

            report.append(String.format("📦 本次购买：%s (%.2f元) - %s\n",
                    order.getProductId(), order.getAmount(), timeGap));

            report.append(String.format("🛒 最近购买：%s\n",
                    String.join(" → ", recentProducts)));

            report.append(String.format("❤️ 偏好品类：%s (购买%d次)\n",
                    favoriteCategory, maxCount));

            report.append(String.format("💰 累计消费：%.2f元\n", totalAmount));

            // 用户等级判断
            String level;
            if (totalAmount >= 50000) {
                level = "💎 钻石会员";
            } else if (totalAmount >= 10000) {
                level = "🌟 黄金会员";
            } else if (totalAmount >= 5000) {
                level = "⭐ 银牌会员";
            } else {
                level = "👤 普通会员";
            }
            report.append(String.format("🏆 会员等级：%s\n", level));

            report.append("=".repeat(60) + "\n");

            out.collect(report.toString());
        }
    }

    /**
     * 模拟订单源
     */
    public static class OrderSource implements SourceFunction<Order> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"Alice", "Bob", "Charlie"};
        private final String[] products = {"iPhone", "MacBook", "iPad", "AirPods", "Watch"};
        private final String[] categories = {"电子产品", "电子产品", "电子产品", "电子产品", "电子产品"};
        private final String[] cities = {"北京", "上海", "广州"};
        private int orderIdCounter = 1;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while (running) {
                String orderId = "order" + orderIdCounter++;
                String userId = users[random.nextInt(users.length)];
                String productId = products[random.nextInt(products.length)];
                String category = categories[random.nextInt(categories.length)];
                String city = cities[random.nextInt(cities.length)];
                int quantity = 1;
                double amount = (random.nextInt(5000) + 500) * quantity;
                long timestamp = System.currentTimeMillis();

                Order order = new Order(orderId, userId, productId, category,
                        city, quantity, amount, timestamp);
                ctx.collect(order);

                Thread.sleep(3000);  // 每3秒一个订单
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
