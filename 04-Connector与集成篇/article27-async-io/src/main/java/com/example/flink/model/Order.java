package com.example.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;      // 订单 ID
    private String userId;       // 用户 ID
    private String productId;    // 商品 ID
    private double amount;       // 订单金额
    private long timestamp;      // 订单时间戳

    public Order() {
    }

    public Order(String orderId, String userId, String productId, double amount, long timestamp) {
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

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
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
        return Double.compare(order.amount, amount) == 0 &&
                timestamp == order.timestamp &&
                Objects.equals(orderId, order.orderId) &&
                Objects.equals(userId, order.userId) &&
                Objects.equals(productId, order.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, productId, amount, timestamp);
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
