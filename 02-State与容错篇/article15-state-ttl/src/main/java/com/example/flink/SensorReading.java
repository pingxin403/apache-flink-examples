package com.example.flink;

import java.io.Serializable;

/**
 * 传感器读数
 */
public class SensorReading implements Serializable {
    private String deviceId;
    private double value;
    private long timestamp;
    
    public SensorReading() {}
    
    public SensorReading(String deviceId, double value, long timestamp) {
        this.deviceId = deviceId;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getDeviceId() {
        return deviceId;
    }
    
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
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
        return "SensorReading{" +
                "deviceId='" + deviceId + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
