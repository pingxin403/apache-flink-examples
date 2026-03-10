package com.example.flink;

import java.io.Serializable;

/**
 * 用户事件
 */
public class UserEvent implements Serializable {
    private String userId;
    private String eventType;
    private long timestamp;
    
    public UserEvent() {}
    
    public UserEvent(String userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
