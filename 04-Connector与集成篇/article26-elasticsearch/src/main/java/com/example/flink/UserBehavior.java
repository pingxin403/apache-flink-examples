package com.example.flink;

import java.io.Serializable;

/**
 * 用户行为数据模型
 * 
 * 用于表示用户在电商平台的行为事件，包括浏览、点击、加购、购买等
 */
public class UserBehavior implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String userId;      // 用户ID
    private String action;      // 行为类型：view, click, cart, purchase
    private String productId;   // 商品ID
    private String category;    // 商品类目
    private Long timestamp;     // 事件时间戳（毫秒）
    
    public UserBehavior() {
    }
    
    public UserBehavior(String userId, String action, String productId, 
                       String category, Long timestamp) {
        this.userId = userId;
        this.action = action;
        this.productId = productId;
        this.category = category;
        this.timestamp = timestamp;
    }
    
    // Getters and Setters
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getAction() {
        return action;
    }
    
    public void setAction(String action) {
        this.action = action;
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
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                ", productId='" + productId + '\'' +
                ", category='" + category + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
