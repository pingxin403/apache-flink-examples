package com.example.flink.model;

import java.io.Serializable;

/**
 * 配置规则类
 * 用于 BroadcastState 示例
 */
public class ConfigRule implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String ruleId;
    private String ruleType;  // "threshold" | "pattern"
    private double threshold;
    private String pattern;
    
    public ConfigRule() {
    }
    
    public ConfigRule(String ruleId, String ruleType, double threshold) {
        this.ruleId = ruleId;
        this.ruleType = ruleType;
        this.threshold = threshold;
    }
    
    public ConfigRule(String ruleId, String ruleType, String pattern) {
        this.ruleId = ruleId;
        this.ruleType = ruleType;
        this.pattern = pattern;
    }
    
    public String getRuleId() {
        return ruleId;
    }
    
    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }
    
    public String getRuleType() {
        return ruleType;
    }
    
    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }
    
    public double getThreshold() {
        return threshold;
    }
    
    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }
    
    public String getPattern() {
        return pattern;
    }
    
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
    
    @Override
    public String toString() {
        return "ConfigRule{" +
                "ruleId='" + ruleId + '\'' +
                ", ruleType='" + ruleType + '\'' +
                ", threshold=" + threshold +
                ", pattern='" + pattern + '\'' +
                '}';
    }
}
