package com.example.flink;

import java.io.Serializable;

/**
 * 用户会话
 */
public class UserSession implements Serializable {
    private String userId;
    private long startTime;
    private long lastActivityTime;
    private int eventCount;
    
    public UserSession() {}
    
    public UserSession(String userId, long startTime) {
        this.userId = userId;
        this.startTime = startTime;
        this.lastActivityTime = startTime;
        this.eventCount = 0;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getLastActivityTime() {
        return lastActivityTime;
    }
    
    public void setLastActivityTime(long lastActivityTime) {
        this.lastActivityTime = lastActivityTime;
    }
    
    public int getEventCount() {
        return eventCount;
    }
    
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }
    
    public void incrementEventCount() {
        this.eventCount++;
    }
    
    @Override
    public String toString() {
        return "UserSession{" +
                "userId='" + userId + '\'' +
                ", startTime=" + startTime +
                ", lastActivityTime=" + lastActivityTime +
                ", eventCount=" + eventCount +
                '}';
    }
}
