package com.example.flink;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单 POJO 类
 *
 * 对应 MySQL 中的 orders 表结构：
 * - id: 订单ID（主键，自增）
 * - userId: 用户ID
 * - productName: 商品名称
 * - amount: 订单金额
 * - status: 订单状态（CREATED/PAID/SHIPPED/COMPLETED/CANCELLED）
 * - createTime: 创建时间
 * - updateTime: 更新时间
 */
public class Order implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;
    private String userId;
    private String productName;
    private BigDecimal amount;
    private String status;
    private String createTime;
    private String updateTime;

    public Order() {
    }

    public Order(Long id, String userId, String productName,
                 BigDecimal amount, String status) {
        this.id = id;
        this.userId = userId;
        this.productName = productName;
        this.amount = amount;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", userId='" + userId + '\'' +
                ", productName='" + productName + '\'' +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }
}
