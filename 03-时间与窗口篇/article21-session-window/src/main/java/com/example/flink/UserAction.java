package com.example.flink;

/**
 * 用户行为事件
 */
public class UserAction {
    private String userId;          // 用户 ID
    private String actionType;      // 行为类型: VIEW/ADD_CART/ORDER
    private String productId;       // 商品 ID
    private Double price;           // 商品价格
    private Long timestamp;         // 事件时间

    public UserAction() {
    }

    public UserAction(String userId, String actionType, String productId, 
                     Double price, Long timestamp) {
        this.userId = userId;
        this.actionType = actionType;
        this.productId = productId;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", actionType='" + actionType + '\'' +
                ", productId='" + productId + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }
}
