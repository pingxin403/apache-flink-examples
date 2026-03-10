package com.example.flink;

import java.io.Serializable;

/**
 * 会话告警
 */
public class SessionAlert implements Serializable {
    private String userId;
    private String message;
    private long timestamp;
    
    public SessionAlert() {}
    
    public SessionAlert(String userId, String message, long timestamp) {
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
        return "SessionAlert{" +
                "userId='" + userId + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
