package com.example.flink;

import java.io.Serializable;
import java.util.Objects;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;      // 订单ID
    private String userId;       // 用户ID
    private String productId;    // 商品ID
    private Double amount;       // 订单金额
    private Long timestamp;      // 订单时间戳

    public Order() {
    }

    public Order(String orderId, String userId, String productId, Double amount, Long timestamp) {
        this.orderId = orderId;
        this.userId = userId;
        this.productId = productId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    // Getters and Setters
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(orderId, order.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId);
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
