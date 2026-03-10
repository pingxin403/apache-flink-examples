package com.example.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 交易数据模型
 */
public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private String transactionId;  // 交易 ID
    private String userId;         // 用户 ID
    private double amount;         // 交易金额
    private String merchantId;     // 商户 ID
    private long timestamp;        // 交易时间戳

    public Transaction() {
    }

    public Transaction(String transactionId, String userId, double amount, 
                      String merchantId, long timestamp) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Double.compare(that.amount, amount) == 0 &&
                timestamp == that.timestamp &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(merchantId, that.merchantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, userId, amount, merchantId, timestamp);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", merchantId='" + merchantId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
