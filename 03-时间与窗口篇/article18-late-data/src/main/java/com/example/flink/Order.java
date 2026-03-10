package com.example.flink;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 订单实体类
 */
public class Order {
    private String orderId;      // 订单ID
    private String userId;       // 用户ID
    private BigDecimal amount;   // 订单金额
    private long timestamp;      // 事件时间戳

    public Order() {
    }

    public Order(String orderId, String userId, BigDecimal amount, long timestamp) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return timestamp == order.timestamp &&
                Objects.equals(orderId, order.orderId) &&
                Objects.equals(userId, order.userId) &&
                Objects.equals(amount, order.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, amount, timestamp);
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
