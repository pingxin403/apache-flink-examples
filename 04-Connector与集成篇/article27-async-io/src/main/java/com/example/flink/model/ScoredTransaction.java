package com.example.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 风控评分后的交易
 */
public class ScoredTransaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private Transaction transaction;  // 原始交易
    private int riskScore;           // 风险评分(0-100,越高越危险)

    public ScoredTransaction() {
    }

    public ScoredTransaction(Transaction transaction, int riskScore) {
        this.transaction = transaction;
        this.riskScore = riskScore;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public int getRiskScore() {
        return riskScore;
    }

    public void setRiskScore(int riskScore) {
        this.riskScore = riskScore;
    }

    /**
     * 判断是否为高风险交易
     */
    public boolean isHighRisk() {
        return riskScore >= 80;
    }

    /**
     * 判断是否为中风险交易
     */
    public boolean isMediumRisk() {
        return riskScore >= 50 && riskScore < 80;
    }

    /**
     * 判断是否为低风险交易
     */
    public boolean isLowRisk() {
        return riskScore < 50;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScoredTransaction that = (ScoredTransaction) o;
        return riskScore == that.riskScore &&
                Objects.equals(transaction, that.transaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, riskScore);
    }

    @Override
    public String toString() {
        return "ScoredTransaction{" +
                "transaction=" + transaction +
                ", riskScore=" + riskScore +
                ", riskLevel=" + getRiskLevel() +
                '}';
    }

    private String getRiskLevel() {
        if (isHighRisk()) return "HIGH";
        if (isMediumRisk()) return "MEDIUM";
        return "LOW";
    }
}
