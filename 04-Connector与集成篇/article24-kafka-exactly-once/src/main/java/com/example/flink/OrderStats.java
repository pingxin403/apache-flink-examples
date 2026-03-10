package com.example.flink;

import java.io.Serializable;

/**
 * 订单统计结果
 */
public class OrderStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;           // 用户ID
    private Long windowStart;        // 窗口开始时间
    private Long windowEnd;          // 窗口结束时间
    private Long orderCount;         // 订单数量
    private Double totalAmount;      // 总金额
    private Double avgAmount;        // 平均金额

    public OrderStats() {
    }

    public OrderStats(String userId, Long windowStart, Long windowEnd, 
                     Long orderCount, Double totalAmount, Double avgAmount) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.orderCount = orderCount;
        this.totalAmount = totalAmount;
        this.avgAmount = avgAmount;
    }

    // Getters and Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Double getAvgAmount() {
        return avgAmount;
    }

    public void setAvgAmount(Double avgAmount) {
        this.avgAmount = avgAmount;
    }

    @Override
    public String toString() {
        return "OrderStats{" +
                "userId='" + userId + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", orderCount=" + orderCount +
                ", totalAmount=" + totalAmount +
                ", avgAmount=" + avgAmount +
                '}';
    }
}
