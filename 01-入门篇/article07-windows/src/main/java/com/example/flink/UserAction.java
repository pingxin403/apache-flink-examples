package com.example.flink;

import java.io.Serializable;

/**
 * 用户行为数据模型
 */
public class UserAction implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;       // 用户ID
    private String action;       // 行为类型（click, view, purchase等）
    private String page;         // 页面
    private Long timestamp;      // 行为时间戳（毫秒）

    public UserAction() {
    }

    public UserAction(String userId, String action, String page, Long timestamp) {
        this.userId = userId;
        this.action = action;
        this.page = page;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                ", page='" + page + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
