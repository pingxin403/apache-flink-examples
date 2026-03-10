package com.example.flink;

import com.example.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * MapState 示例：统计商品各城市销量分布
 * 
 * 功能：为每个商品维护一个城市->销量的映射
 */
public class MapStateExample {

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

        // 按商品ID分组，统计各城市销量
        DataStream<String> salesReports = orderStream
                .keyBy(Order::getProductId)
                .process(new ProductSalesAnalyzer());

        salesReports.print();

        env.execute("MapState Example - Product Sales Analyzer");
    }

    /**
     * 商品销量分析器
     * 使用 MapState 记录每个城市的销量
     */
    public static class ProductSalesAnalyzer extends KeyedProcessFunction<String, Order, String> {

        // 状态：城市 -> 销量
        private MapState<String, Integer> citySalesState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("citySales", String.class, Integer.class);
            citySalesState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out)
                throws Exception {
            String city = order.getCity();

            // 读取该城市的当前销量
            Integer currentSales = citySalesState.get(city);
            if (currentSales == null) {
                currentSales = 0;
            }

            // 更新销量
            citySalesState.put(city, currentSales + order.getQuantity());

            // 生成报表
            StringBuilder report = new StringBuilder();
            report.append(String.format("\n📈 商品 %s 销量分布：\n", order.getProductId()));

            int totalSales = 0;
            Map<String, Integer> allSales = new TreeMap<>();  // 使用TreeMap排序

            // 遍历所有城市销量
            for (Map.Entry<String, Integer> entry : citySalesState.entries()) {
                allSales.put(entry.getKey(), entry.getValue());
                totalSales += entry.getValue();
            }

            // 输出各城市销量和占比
            for (Map.Entry<String, Integer> entry : allSales.entrySet()) {
                double percentage = (entry.getValue() * 100.0) / totalSales;
                report.append(String.format("  %s: %d件 (%.1f%%)\n",
                        entry.getKey(), entry.getValue(), percentage));
            }

            report.append(String.format("  总销量: %d件\n", totalSales));

            out.collect(report.toString());
        }
    }

    /**
     * 模拟订单源
     */
    public static class OrderSource implements SourceFunction<Order> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"user1", "user2", "user3", "user4", "user5"};
        private final String[] products = {"iPhone15", "MacBook", "iPad"};
        private final String[] categories = {"电子产品", "电子产品", "电子产品"};
        private final String[] cities = {"北京", "上海", "广州", "深圳", "杭州"};
        private int orderIdCounter = 1;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while (running) {
                String orderId = "order" + orderIdCounter++;
                String userId = users[random.nextInt(users.length)];
                int productIndex = random.nextInt(products.length);
                String productId = products[productIndex];
                String category = categories[productIndex];
                String city = cities[random.nextInt(cities.length)];
                int quantity = random.nextInt(3) + 1;
                double amount = (random.nextInt(5000) + 1000) * quantity;
                long timestamp = System.currentTimeMillis();

                Order order = new Order(orderId, userId, productId, category,
                        city, quantity, amount, timestamp);
                ctx.collect(order);

                Thread.sleep(2000);  // 每2秒一个订单
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
