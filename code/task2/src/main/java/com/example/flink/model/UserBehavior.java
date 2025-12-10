package com.example.flink.model;

import java.io.Serializable;

/**
 * 用户行为数据模型
 */
public class UserBehavior implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;        // 用户ID
    private String productId;     // 商品ID
    private String behaviorType;  // 行为类型：click, order, pay
    private long timestamp;       // 行为时间戳
    private double amount;        // 金额（下单/支付有效，点击为0）

    // 无参构造（Flink POJO要求）
    public UserBehavior() {}

    // 全参构造
    public UserBehavior(String userId, String productId, String behaviorType, long timestamp, double amount) {
        this.userId = userId;
        this.productId = productId;
        this.behaviorType = behaviorType;
        this.timestamp = timestamp;
        this.amount = amount;
    }

    // getter和setter方法
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getBehaviorType() {
        return behaviorType;
    }

    public void setBehaviorType(String behaviorType) {
        this.behaviorType = behaviorType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", behaviorType='" + behaviorType + '\'' +
                ", timestamp=" + timestamp +
                ", amount=" + amount +
                '}';
    }
}