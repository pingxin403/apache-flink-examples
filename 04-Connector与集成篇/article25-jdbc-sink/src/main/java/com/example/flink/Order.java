package com.example.flink;

import java.io.Serializable;
import java.util.Objects;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;          // 订单ID
    private String userId;      // 用户ID
    private Double amount;      // 订单金额

    public Order() {
    }

    public Order(String id, String userId, Double amount) {
        this.id = id;
        this.userId = userId;
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(id, order.id) &&
                Objects.equals(userId, order.userId) &&
                Objects.equals(amount, order.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userId, amount);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id='" + id + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
