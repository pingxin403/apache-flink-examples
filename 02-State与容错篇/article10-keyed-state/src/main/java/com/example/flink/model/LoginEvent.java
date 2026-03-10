package com.example.flink.model;

import java.io.Serializable;

/**
 * 登录事件
 */
public class LoginEvent implements Serializable {
    private String userId;
    private long timestamp;
    private String ip;

    public LoginEvent() {
    }

    public LoginEvent(String userId, long timestamp, String ip) {
        this.userId = userId;
        this.timestamp = timestamp;
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", ip='" + ip + '\'' +
                '}';
    }
}
