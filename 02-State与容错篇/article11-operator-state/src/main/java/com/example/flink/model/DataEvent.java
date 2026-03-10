package com.example.flink.model;

import java.io.Serializable;

/**
 * 数据事件类
 * 用于 BroadcastState 示例
 */
public class DataEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String userId;
    private double value;
    private long timestamp;
    
    public DataEvent() {
    }
    
    public DataEvent(String userId, double value, long timestamp) {
        this.userId = userId;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public double getValue() {
        return value;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "DataEvent{" +
                "userId='" + userId + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
