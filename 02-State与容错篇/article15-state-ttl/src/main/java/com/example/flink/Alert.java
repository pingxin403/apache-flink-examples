package com.example.flink;

import java.io.Serializable;

/**
 * 告警信息
 */
public class Alert implements Serializable {
    private String deviceId;
    private String message;
    private long timestamp;
    
    public Alert() {}
    
    public Alert(String deviceId, String message, long timestamp) {
        this.deviceId = deviceId;
        this.message = message;
        this.timestamp = timestamp;
    }
    
    public String getDeviceId() {
        return deviceId;
    }
    
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
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
                "deviceId='" + deviceId + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
