package com.example.flink;

import com.example.flink.client.AsyncRiskServiceClient;
import com.example.flink.model.ScoredTransaction;
import com.example.flink.model.Transaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 有序 vs 无序模式对比示例
 * 
 * 演示有序模式和无序模式在性能上的差异
 */
public class OrderedVsUnorderedExample {
    
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建交易数据流
        DataStream<Transaction> transactions = env.addSource(new TransactionSource());
        
        // 方式 1: 有序模式
        DataStream<ScoredTransaction> orderedResults = AsyncDataStream.orderedWait(
            transactions,
            new AsyncRiskCheckFunction("ORDERED"),
            5000,
            TimeUnit.MILLISECONDS,
            100
        );
        
        // 方式 2: 无序模式
        DataStream<ScoredTransaction> unorderedResults = AsyncDataStream.unorderedWait(
            transactions,
            new AsyncRiskCheckFunction("UNORDERED"),
            5000,
            TimeUnit.MILLISECONDS,
            100
        );
        
        // 打印结果
        System.out.println("=== Ordered Mode Results ===");
        orderedResults.print();
        
        System.out.println("\n=== Unordered Mode Results ===");
        unorderedResults.print();
        
        // 执行作业
        env.execute("Ordered vs Unordered Example");
    }
    
    /**
     * 异步风控检查函数(带模式标识)
     */
    public static class AsyncRiskCheckFunction 
            extends RichAsyncFunction<Transaction, ScoredTransaction> {
        
        private transient AsyncRiskServiceClient client;
        private final String mode;
        private long startTime;
        private int processedCount;
        
        public AsyncRiskCheckFunction(String mode) {
            this.mode = mode;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            client = new AsyncRiskServiceClient();
            startTime = System.currentTimeMillis();
            processedCount = 0;
        }
        
        @Override
        public void asyncInvoke(Transaction tx, ResultFuture<ScoredTransaction> resultFuture) 
                throws Exception {
            
            long requestStartTime = System.currentTimeMillis();
            
            // 模拟响应时间波动:10% 的请求会很慢
            Random random = new Random();
            boolean isSlow = random.nextInt(10) == 0;
            
            if (isSlow) {
                // 慢请求:延迟 500ms
                client.checkRiskAsyncSlow(tx.getUserId(), tx.getAmount(), 500)
                    .whenComplete((score, throwable) -> {
                        handleResult(tx, score, throwable, resultFuture, 
                                   requestStartTime, true);
                    });
            } else {
                // 正常请求:延迟 50ms
                client.checkRiskAsync(tx.getUserId(), tx.getAmount())
                    .whenComplete((score, throwable) -> {
                        handleResult(tx, score, throwable, resultFuture, 
                                   requestStartTime, false);
                    });
            }
        }
        
        private void handleResult(
                Transaction tx,
                Integer score,
                Throwable throwable,
                ResultFuture<ScoredTransaction> resultFuture,
                long requestStartTime,
                boolean wasSlow) {
            
            processedCount++;
            long latency = System.currentTimeMillis() - requestStartTime;
            
            if (throwable != null) {
                resultFuture.complete(
                    Collections.singleton(new ScoredTransaction(tx, -1))
                );
            } else {
                resultFuture.complete(
                    Collections.singleton(new ScoredTransaction(tx, score))
                );
            }
            
            // 每处理 100 条记录,打印统计信息
            if (processedCount % 100 == 0) {
                long totalTime = System.currentTimeMillis() - startTime;
                double throughput = processedCount * 1000.0 / totalTime;
                
                System.out.println(String.format(
                    "[%s] Processed: %d, Throughput: %.2f QPS, " +
                    "Current Latency: %dms (slow: %s)",
                    mode, processedCount, throughput, latency, wasSlow
                ));
            }
        }
        
        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
            
            // 打印最终统计
            long totalTime = System.currentTimeMillis() - startTime;
            double avgThroughput = processedCount * 1000.0 / totalTime;
            
            System.out.println(String.format(
                "\n[%s] Final Stats - Total: %d, Time: %dms, Avg Throughput: %.2f QPS",
                mode, processedCount, totalTime, avgThroughput
            ));
        }
    }
    
    /**
     * 交易数据源
     */
    public static class TransactionSource implements SourceFunction<Transaction> {
        
        private volatile boolean running = true;
        private final Random random = new Random();
        
        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            int count = 0;
            while (running && count < 1000) {
                Transaction tx = new Transaction(
                    "tx-" + count,
                    "user-" + random.nextInt(100),
                    1000 + random.nextDouble() * 9000,
                    "merchant-" + random.nextInt(20),
                    System.currentTimeMillis()
                );
                
                ctx.collect(tx);
                count++;
                
                // 控制发送速率:每秒 100 条
                Thread.sleep(10);
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
