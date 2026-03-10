package com.example.flink.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 事件数据模型
 * 用于演示 Watermark 生成策略
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private String userId;          // 用户ID
    private String eventType;       // 事件类型
    private long timestamp;         // 事件时间戳(毫秒)
    private boolean watermarkMarker; // 是否为 Watermark 标记事件

    public Event() {
    }

    public Event(String userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.watermarkMarker = false;
    }

    public Event(String userId, String eventType, long timestamp, boolean watermarkMarker) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.watermarkMarker = watermarkMarker;
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

    public boolean isWatermarkMarker() {
        return watermarkMarker;
    }

    public void setWatermarkMarker(boolean watermarkMarker) {
        this.watermarkMarker = watermarkMarker;
    }

    /**
     * 获取格式化的时间字符串
     */
    public String getFormattedTime() {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timestamp),
            ZoneId.systemDefault()
        );
        return dateTime.format(formatter);
    }

    @Override
    public String toString() {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", time=" + getFormattedTime() +
                ", watermarkMarker=" + watermarkMarker +
                '}';
    }
}
