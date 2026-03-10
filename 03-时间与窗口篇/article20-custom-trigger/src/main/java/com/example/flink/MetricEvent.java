package com.example.flink;

import java.io.Serializable;

/**
 * 监控指标事件
 */
public class MetricEvent implements Serializable {
    private String serviceName;  // 服务名称
    private long timestamp;      // 事件时间戳
    private long qps;            // 每秒请求数
    private double errorRate;    // 错误率
    private double value;        // 指标值

    public MetricEvent() {
    }

    public MetricEvent(String serviceName, long timestamp, long qps, double errorRate, double value) {
        this.serviceName = serviceName;
        this.timestamp = timestamp;
        this.qps = qps;
        this.errorRate = errorRate;
        this.value = value;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getQps() {
        return qps;
    }

    public void setQps(long qps) {
        this.qps = qps;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }


    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "serviceName='" + serviceName + '\'' +
                ", timestamp=" + timestamp +
                ", qps=" + qps +
                ", errorRate=" + errorRate +
                ", value=" + value +
                '}';
    }
}
