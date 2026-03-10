package com.example.flink;

import java.io.Serializable;

/**
 * 订单实体类
 * 用于演示 DataStream 算子操作
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;      // 用户 ID
    private Double amount;      // 订单金额
    private Long timestamp;     // 订单时间戳

    // 无参构造函数（Flink 序列化需要）
    public Order() {
    }

    // 全参构造函数
    public Order(String userId, Double amount, Long timestamp) {
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    // Getter 和 Setter 方法
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
