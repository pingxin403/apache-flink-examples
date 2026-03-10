package com.example.flink;

import com.example.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * ReducingState 示例：计算用户累计消费金额
 * 
 * 功能：自动累加用户消费金额，达到VIP标准时触发升级
 */
public class ReducingStateExample {

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

        // 按用户ID分组，追踪累计消费
        DataStream<String> vipUpgrades = orderStream
                .keyBy(Order::getUserId)
                .process(new UserSpendingTracker());

        vipUpgrades.print();

        env.execute("ReducingState Example - User Spending Tracker");
    }

    /**
     * 用户消费追踪器
     * 使用 ReducingState 自动累加消费金额
     */
    public static class UserSpendingTracker extends KeyedProcessFunction<String, Order, String> {

        // 状态：累计消费金额
        private ReducingState<Double> totalSpendingState;

        // VIP等级阈值
        private static final double VIP_THRESHOLD = 10000.0;
        private static final double SVIP_THRESHOLD = 50000.0;

        @Override
        public void open(Configuration parameters) {
            ReducingStateDescriptor<Double> descriptor =
                    new ReducingStateDescriptor<>(
                            "totalSpending",
                            new ReduceFunction<Double>() {
                                @Override
                                public Double reduce(Double value1, Double value2) {
                                    return value1 + value2;  // 累加
                                }
                            },
                            Double.class
                    );
            totalSpendingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out)
                throws Exception {
            // 添加新金额（自动累加）
            totalSpendingState.add(order.getAmount());

            // 读取累计金额
            Double totalSpending = totalSpendingState.get();

            // 生成消费报告
            String report = String.format(
                    "💰 用户 %s 新增消费 %.2f元，累计消费 %.2f元",
                    order.getUserId(),
                    order.getAmount(),
                    totalSpending
            );

            // 检查是否达到VIP标准
            if (totalSpending >= SVIP_THRESHOLD) {
                report += " 🌟 已达到超级VIP标准！";
            } else if (totalSpending >= VIP_THRESHOLD) {
                report += " ⭐ 已达到VIP标准！";
            } else {
                double remaining = VIP_THRESHOLD - totalSpending;
                report += String.format(" (距离VIP还差 %.2f元)", remaining);
            }

            out.collect(report);
        }
    }

    /**
     * 模拟订单源
     */
    public static class OrderSource implements SourceFunction<Order> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"Alice", "Bob", "Charlie"};
        private final String[] products = {"prod1", "prod2", "prod3", "prod4"};
        private final String[] categories = {"电子产品", "图书", "服装", "食品"};
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
                int quantity = random.nextInt(3) + 1;
                
                // 生成不同金额范围的订单
                double amount;
                if (random.nextDouble() < 0.1) {
                    // 10%概率生成大额订单
                    amount = (random.nextInt(5000) + 1000) * quantity;
                } else {
                    // 90%概率生成普通订单
                    amount = (random.nextInt(500) + 50) * quantity;
                }
                
                long timestamp = System.currentTimeMillis();

                Order order = new Order(orderId, userId, productId, category,
                        city, quantity, amount, timestamp);
                ctx.collect(order);

                Thread.sleep(1500);  // 每1.5秒一个订单
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
