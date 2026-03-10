package com.example.flink.model;

import java.io.Serializable;

/**
 * 订单事件
 */
public class Order implements Serializable {
    private String orderId;
    private String userId;
    private String productId;
    private String category;
    private String city;
    private int quantity;
    private double amount;
    private long timestamp;

    public Order() {
    }

    public Order(String orderId, String userId, String productId, String category, 
                 String city, int quantity, double amount, long timestamp) {
        this.orderId = orderId;
        this.userId = userId;
        this.productId = productId;
        this.category = category;
        this.city = city;
        this.quantity = quantity;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
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
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", category='" + category + '\'' +
                ", city='" + city + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
