package com.example.flink.model;

import java.io.Serializable;

/**
 * 告警类
 * 用于输出检测结果
 */
public class Alert implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String userId;
    private String message;
    private long timestamp;
    
    public Alert() {
    }
    
    public Alert(String userId, String message) {
        this.userId = userId;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
    }
    
    public Alert(String userId, String message, long timestamp) {
        this.userId = userId;
        this.message = message;
        this.timestamp = timestamp;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "Alert{" +
                "userId='" + userId + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
