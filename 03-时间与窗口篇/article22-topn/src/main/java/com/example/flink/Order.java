package com.example.flink;

import java.io.Serializable;

/**
 * 订单数据模型
 */
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;        // 订单ID
    private String productId;      // 商品ID
    private String productName;    // 商品名称
    private Integer quantity;      // 购买数量
    private Long timestamp;        // 订单时间戳

    public Order() {
    }

    public Order(String orderId, String productId, String productName, 
                 Integer quantity, Long timestamp) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
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

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
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
                ", productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", timestamp=" + timestamp +
                '}';
    }
}
