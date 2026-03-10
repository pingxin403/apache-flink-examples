package com.example.flink;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单数据模型
 */
public class Order {
    private String orderId;
    private String userId;
    private String productId;
    private BigDecimal amount;
    private LocalDateTime orderTime;

    public Order() {
    }

    public Order(String orderId, String userId, String productId, BigDecimal amount, LocalDateTime orderTime) {
        this.orderId = orderId;
        this.userId = userId;
        this.productId = productId;
        this.amount = amount;
        this.orderTime = orderTime;
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

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public LocalDateTime getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(LocalDateTime orderTime) {
        this.orderTime = orderTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", amount=" + amount +
                ", orderTime=" + orderTime +
                '}';
    }
}
