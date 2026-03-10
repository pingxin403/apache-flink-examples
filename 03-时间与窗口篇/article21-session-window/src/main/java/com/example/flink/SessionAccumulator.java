package com.example.flink;

import java.util.ArrayList;
import java.util.List;

/**
 * 会话累加器
 * 用于增量聚合会话数据
 */
public class SessionAccumulator {
    public Long sessionStart;
    public Long sessionEnd;
    public Integer viewCount = 0;
    public Integer addCartCount = 0;
    public Integer orderCount = 0;
    public Double totalAmount = 0.0;
    public List<String> viewPath = new ArrayList<>();

    public SessionAccumulator() {
    }

    @Override
    public String toString() {
        return "SessionAccumulator{" +
                "sessionStart=" + sessionStart +
                ", sessionEnd=" + sessionEnd +
                ", viewCount=" + viewCount +
                ", addCartCount=" + addCartCount +
                ", orderCount=" + orderCount +
                ", totalAmount=" + totalAmount +
                ", viewPath=" + viewPath +
                '}';
    }
}
