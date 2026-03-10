package com.example.flink;

import java.util.List;

/**
 * 会话统计结果
 */
public class SessionStats {
    private String userId;              // 用户 ID
    private Long sessionStart;          // 会话开始时间
    private Long sessionEnd;            // 会话结束时间
    private Long duration;              // 会话时长(秒)
    private Integer viewCount;          // 浏览次数
    private Integer addCartCount;       // 加购次数
    private Integer orderCount;         // 下单次数
    private Double totalAmount;         // 总金额
    private List<String> viewPath;      // 浏览路径
    private Double addCartRate;         // 加购率
    private Double orderRate;           // 下单率

    public SessionStats() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getSessionStart() {
        return sessionStart;
    }

    public void setSessionStart(Long sessionStart) {
        this.sessionStart = sessionStart;
    }

    public Long getSessionEnd() {
        return sessionEnd;
    }

    public void setSessionEnd(Long sessionEnd) {
        this.sessionEnd = sessionEnd;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Integer getViewCount() {
        return viewCount;
    }

    public void setViewCount(Integer viewCount) {
        this.viewCount = viewCount;
    }

    public Integer getAddCartCount() {
        return addCartCount;
    }

    public void setAddCartCount(Integer addCartCount) {
        this.addCartCount = addCartCount;
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public List<String> getViewPath() {
        return viewPath;
    }

    public void setViewPath(List<String> viewPath) {
        this.viewPath = viewPath;
    }

    public Double getAddCartRate() {
        return addCartRate;
    }

    public void setAddCartRate(Double addCartRate) {
        this.addCartRate = addCartRate;
    }

    public Double getOrderRate() {
        return orderRate;
    }

    public void setOrderRate(Double orderRate) {
        this.orderRate = orderRate;
    }

    @Override
    public String toString() {
        return "SessionStats{" +
                "userId='" + userId + '\'' +
                ", sessionStart=" + sessionStart +
                ", sessionEnd=" + sessionEnd +
                ", duration=" + duration +
                ", viewCount=" + viewCount +
                ", addCartCount=" + addCartCount +
                ", orderCount=" + orderCount +
                ", totalAmount=" + totalAmount +
                ", viewPath=" + viewPath +
                ", addCartRate=" + addCartRate +
                ", orderRate=" + orderRate +
                '}';
    }
}
