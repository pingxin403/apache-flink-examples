package com.example.flink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 订单统计结果
 */
public class OrderStats implements Serializable {
    private String key;
    private long windowStart;
    private long windowEnd;
    private BigDecimal totalAmount;
    private int count;
    private BigDecimal avgAmount;

    public OrderStats() {
    }

    public OrderStats(String key, long windowStart, long windowEnd, 
                     BigDecimal totalAmount, int count, BigDecimal avgAmount) {
        this.key = key;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalAmount = totalAmount;
        this.count = count;
        this.avgAmount = avgAmount;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public BigDecimal getAvgAmount() {
        return avgAmount;
    }

    public void setAvgAmount(BigDecimal avgAmount) {
        this.avgAmount = avgAmount;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "OrderStats{" +
                "key='" + key + '\'' +
                ", window=[" + sdf.format(new Date(windowStart)) + 
                " ~ " + sdf.format(new Date(windowEnd)) + "]" +
                ", totalAmount=" + totalAmount +
                ", count=" + count +
                ", avgAmount=" + avgAmount +
                '}';
    }
}
