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
import java.util.concurrent.TimeoutException;

/**
 * 异步风控调用示例
 * 
 * 演示如何使用异步 I/O 调用外部风控服务进行交易评分
 */
public class AsyncRiskCheckExample {
    
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建交易数据流
        DataStream<Transaction> transactions = env.addSource(new TransactionSource());
        
        // 应用异步 I/O 进行风控检查
        DataStream<ScoredTransaction> scoredTransactions = AsyncDataStream.unorderedWait(
            transactions,
            new AsyncRiskCheckFunction(),
            3000,  // 超时时间 3 秒
            TimeUnit.MILLISECONDS,
            100    // 最大并发请求数
        );
        
        // 过滤高风险交易
        DataStream<ScoredTransaction> highRiskTransactions = scoredTransactions
            .filter(ScoredTransaction::isHighRisk);
        
        // 打印结果
        System.out.println("=== All Scored Transactions ===");
        scoredTransactions.print();
        
        System.out.println("\n=== High Risk Transactions ===");
        highRiskTransactions.print();
        
        // 执行作业
        env.execute("Async Risk Check Example");
    }
    
    /**
     * 异步风控检查函数
     */
    public static class AsyncRiskCheckFunction 
            extends RichAsyncFunction<Transaction, ScoredTransaction> {
        
        private transient AsyncRiskServiceClient client;
        private static final int MAX_RETRIES = 3;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化异步风控服务客户端
            client = new AsyncRiskServiceClient();
        }
        
        @Override
        public void asyncInvoke(Transaction tx, ResultFuture<ScoredTransaction> resultFuture) 
                throws Exception {
            
            // 发起异步风控检查(带重试)
            asyncInvokeWithRetry(tx, resultFuture, 0);
        }
        
        /**
         * 带重试的异步调用
         */
        private void asyncInvokeWithRetry(
                Transaction tx, 
                ResultFuture<ScoredTransaction> resultFuture,
                int retryCount) {
            
            client.checkRiskAsync(tx.getUserId(), tx.getAmount())
                .whenComplete((score, throwable) -> {
                    if (throwable != null) {
                        if (retryCount < MAX_RETRIES && isRetryable(throwable)) {
                            // 重试
                            System.out.println("Retry " + (retryCount + 1) + 
                                             " for transaction: " + tx.getTransactionId());
                            asyncInvokeWithRetry(tx, resultFuture, retryCount + 1);
                        } else {
                            // 失败,返回默认评分
                            System.err.println("Failed to check risk for transaction: " + 
                                             tx.getTransactionId() + ", error: " + throwable.getMessage());
                            resultFuture.complete(
                                Collections.singleton(new ScoredTransaction(tx, -1))
                            );
                        }
                    } else {
                        // 成功,返回评分结果
                        resultFuture.complete(
                            Collections.singleton(new ScoredTransaction(tx, score))
                        );
                    }
                });
        }
        
        /**
         * 判断异常是否可重试
         */
        private boolean isRetryable(Throwable throwable) {
            // 网络超时、连接异常等可重试
            return throwable instanceof TimeoutException 
                || throwable.getMessage().contains("timeout")
                || throwable.getMessage().contains("connection");
        }
        
        @Override
        public void timeout(Transaction tx, ResultFuture<ScoredTransaction> resultFuture) 
                throws Exception {
            // 超时处理:返回默认评分
            System.err.println("Timeout for transaction: " + tx.getTransactionId());
            resultFuture.complete(
                Collections.singleton(new ScoredTransaction(tx, -1))
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
     * 交易数据源
     */
    public static class TransactionSource implements SourceFunction<Transaction> {
        
        private volatile boolean running = true;
        private final Random random = new Random();
        
        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            int count = 0;
            while (running && count < 1000) {
                // 生成模拟交易
                Transaction tx = new Transaction(
                    "tx-" + count,
                    "user-" + random.nextInt(100),
                    1000 + random.nextDouble() * 9000,
                    "merchant-" + random.nextInt(20),
                    System.currentTimeMillis()
                );
                
                ctx.collect(tx);
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
