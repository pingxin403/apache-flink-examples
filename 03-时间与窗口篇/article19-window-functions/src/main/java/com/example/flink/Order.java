package com.example.flink;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private String orderId;
    private String userId;
    private String productId;
    private BigDecimal amount;
    private long timestamp;

    public Order() {
    }

    public Order(String orderId, String userId, String productId, BigDecimal amount, long timestamp) {
        this.orderId = orderId;
        this.userId = userId;
        this.productId = productId;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
