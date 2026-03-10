package com.example.flink;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 订单统计结果
 */
public class OrderStats {
    private long windowStart;      // 窗口开始时间
    private long windowEnd;        // 窗口结束时间
    private BigDecimal totalAmount; // 订单总额
    private long orderCount;       // 订单数量
    private long updateTime;       // 更新时间戳
    private int version;           // 版本号

    public OrderStats() {
    }

    public OrderStats(long windowStart, long windowEnd, BigDecimal totalAmount, long orderCount) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalAmount = totalAmount;
        this.orderCount = orderCount;
        this.updateTime = System.currentTimeMillis();
        this.version = 1;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "OrderStats{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", totalAmount=" + totalAmount +
                ", orderCount=" + orderCount +
                ", updateTime=" + updateTime +
                ", version=" + version +
                '}';
    }
}
