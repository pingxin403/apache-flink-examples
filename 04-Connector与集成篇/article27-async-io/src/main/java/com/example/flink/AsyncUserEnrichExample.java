package com.example.flink;

import com.example.flink.client.AsyncDatabaseClient;
import com.example.flink.model.EnrichedOrder;
import com.example.flink.model.Order;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 异步维表关联示例
 * 
 * 演示如何使用异步 I/O 从数据库查询用户信息并关联到订单流
 */
public class AsyncUserEnrichExample {
    
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建订单数据流
        DataStream<Order> orders = env.addSource(new OrderSource());
        
        // 应用异步 I/O 进行维表关联
        DataStream<EnrichedOrder> enrichedOrders = AsyncDataStream.unorderedWait(
            orders,
            new AsyncUserEnrichFunction(),
            5000,  // 超时时间 5 秒
            TimeUnit.MILLISECONDS,
            100    // 最大并发请求数
        );
        
        // 打印结果
        enrichedOrders.print();
        
        // 执行作业
        env.execute("Async User Enrich Example");
    }
    
    /**
     * 异步用户信息关联函数
     */
    public static class AsyncUserEnrichFunction 
            extends RichAsyncFunction<Order, EnrichedOrder> {
        
        private transient AsyncDatabaseClient client;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化异步数据库客户端
            // 注意:这里使用模拟客户端,实际使用时需要配置真实的数据库连接
            client = new AsyncDatabaseClient(
                "jdbc:mysql://localhost:3306/test",
                "root",
                "password"
            );
        }
        
        @Override
        public void asyncInvoke(Order order, ResultFuture<EnrichedOrder> resultFuture) 
                throws Exception {
            
            // 发起异步查询
            client.queryUserAsyncMock(order.getUserId())
                .whenComplete((userInfo, throwable) -> {
                    if (throwable != null) {
                        // 查询失败,返回默认值
                        resultFuture.complete(
                            Collections.singleton(
                                new EnrichedOrder(order, "unknown", "unknown")
                            )
                        );
                    } else {
                        // 查询成功,返回关联结果
                        resultFuture.complete(
                            Collections.singleton(
                                new EnrichedOrder(
                                    order,
                                    userInfo.get("level"),
                                    userInfo.get("region")
                                )
                            )
                        );
                    }
                });
        }
        
        @Override
        public void timeout(Order order, ResultFuture<EnrichedOrder> resultFuture) 
                throws Exception {
            // 超时处理:返回默认值
            resultFuture.complete(
                Collections.singleton(
                    new EnrichedOrder(order, "timeout", "timeout")
                )
            );
        }
        
        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
        }
    }
    
    /**
     * 订单数据源
     */
    public static class OrderSource implements SourceFunction<Order> {
        
        private volatile boolean running = true;
        private final Random random = new Random();
        
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            int count = 0;
            while (running && count < 1000) {
                // 生成模拟订单
                Order order = new Order(
                    "order-" + count,
                    "user-" + random.nextInt(100),
                    "product-" + random.nextInt(50),
                    100 + random.nextDouble() * 900,
                    System.currentTimeMillis()
                );
                
                ctx.collect(order);
                count++;
                
                // 控制发送速率
                Thread.sleep(10);
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
