package com.example.flink;

import java.io.Serializable;

/**
 * 订单
 */
public class Order implements Serializable {
    private String orderId;
    private String productId;
    private int quantity;
    private long timestamp;
    
    public Order() {}
    
    public Order(String orderId, String productId, int quantity, long timestamp) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.timestamp = timestamp;
    }
    
    public String getOrderId() {
        return orderId;
    }
    
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    public String getProductId() {
        return productId;
    }
    
    public void setProductId(String productId) {
        this.productId = productId;
    }
    
    public int getQuantity() {
        return quantity;
    }
    
    public void setQuantity(int quantity) {
        this.quantity = quantity;
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
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", timestamp=" + timestamp +
                '}';
    }
}
