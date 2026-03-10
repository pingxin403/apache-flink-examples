package com.example.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 关联用户信息后的订单
 */
public class EnrichedOrder implements Serializable {
    private static final long serialVersionUID = 1L;

    private Order order;         // 原始订单
    private String userLevel;    // 用户等级
    private String userRegion;   // 用户地区

    public EnrichedOrder() {
    }

    public EnrichedOrder(Order order, String userLevel, String userRegion) {
        this.order = order;
        this.userLevel = userLevel;
        this.userRegion = userRegion;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public String getUserLevel() {
        return userLevel;
    }

    public void setUserLevel(String userLevel) {
        this.userLevel = userLevel;
    }

    public String getUserRegion() {
        return userRegion;
    }

    public void setUserRegion(String userRegion) {
        this.userRegion = userRegion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedOrder that = (EnrichedOrder) o;
        return Objects.equals(order, that.order) &&
                Objects.equals(userLevel, that.userLevel) &&
                Objects.equals(userRegion, that.userRegion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(order, userLevel, userRegion);
    }

    @Override
    public String toString() {
        return "EnrichedOrder{" +
                "order=" + order +
                ", userLevel='" + userLevel + '\'' +
                ", userRegion='" + userRegion + '\'' +
                '}';
    }
}
