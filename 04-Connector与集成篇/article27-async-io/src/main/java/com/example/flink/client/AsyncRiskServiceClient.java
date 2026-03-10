package com.example.flink.client;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 异步风控服务客户端
 * 
 * 模拟调用外部风控服务进行交易评分
 */
public class AsyncRiskServiceClient {
    
    private final ExecutorService executorService;
    private final Random random;
    
    public AsyncRiskServiceClient() {
        this.executorService = Executors.newFixedThreadPool(20);
        this.random = new Random();
    }
    
    /**
     * 异步检查交易风险
     * 
     * @param userId 用户 ID
     * @param amount 交易金额
     * @return CompletableFuture 包含风险评分(0-100)
     */
    public CompletableFuture<Integer> checkRiskAsync(String userId, double amount) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 模拟网络延迟(30-100ms)
                Thread.sleep(30 + random.nextInt(70));
                
                // 模拟风控评分逻辑
                int score = calculateRiskScore(userId, amount);
                
                return score;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Risk check interrupted", e);
            }
        }, executorService);
    }
    
    /**
     * 计算风险评分
     * 
     * 简化的风控逻辑:
     * - 金额越大,风险越高
     * - 用户 ID 的哈希值影响基础分数
     * 
     * @param userId 用户 ID
     * @param amount 交易金额
     * @return 风险评分(0-100)
     */
    private int calculateRiskScore(String userId, double amount) {
        // 基础分数(根据用户 ID)
        int baseScore = Math.abs(userId.hashCode()) % 30;
        
        // 金额因子
        int amountFactor = 0;
        if (amount > 10000) {
            amountFactor = 50;
        } else if (amount > 5000) {
            amountFactor = 30;
        } else if (amount > 1000) {
            amountFactor = 15;
        }
        
        // 随机波动
        int randomFactor = random.nextInt(20);
        
        // 总分
        int totalScore = baseScore + amountFactor + randomFactor;
        
        // 限制在 0-100 范围内
        return Math.min(100, Math.max(0, totalScore));
    }
    
    /**
     * 异步检查交易风险(带重试)
     * 
     * @param userId 用户 ID
     * @param amount 交易金额
     * @param maxRetries 最大重试次数
     * @return CompletableFuture 包含风险评分
     */
    public CompletableFuture<Integer> checkRiskAsyncWithRetry(
            String userId, double amount, int maxRetries) {
        
        return checkRiskAsyncWithRetryInternal(userId, amount, 0, maxRetries);
    }
    
    private CompletableFuture<Integer> checkRiskAsyncWithRetryInternal(
            String userId, double amount, int currentRetry, int maxRetries) {
        
        return checkRiskAsync(userId, amount)
            .exceptionally(throwable -> {
                if (currentRetry < maxRetries) {
                    // 重试
                    return checkRiskAsyncWithRetryInternal(
                        userId, amount, currentRetry + 1, maxRetries
                    ).join();
                } else {
                    // 达到最大重试次数,返回默认值
                    return -1;
                }
            });
    }
    
    /**
     * 模拟慢请求(用于测试超时)
     * 
     * @param userId 用户 ID
     * @param amount 交易金额
     * @param delayMs 延迟时间(毫秒)
     * @return CompletableFuture 包含风险评分
     */
    public CompletableFuture<Integer> checkRiskAsyncSlow(
            String userId, double amount, long delayMs) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(delayMs);
                return calculateRiskScore(userId, amount);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Risk check interrupted", e);
            }
        }, executorService);
    }
    
    /**
     * 关闭客户端,释放资源
     */
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
}
