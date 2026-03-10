package com.example.flink;

import java.io.Serializable;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;      // 订单ID
    private String userId;       // 用户ID
    private Double amount;       // 订单金额
    private Long timestamp;      // 订单时间戳（毫秒）

    public Order() {
    }

    public Order(String orderId, String userId, Double amount, Long timestamp) {
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
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
