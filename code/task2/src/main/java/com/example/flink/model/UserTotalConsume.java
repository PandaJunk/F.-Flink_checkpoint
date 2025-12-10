package com.example.flink.model;

import java.io.Serializable;

/**
 * 用户累计消费输出模型
 */
public class UserTotalConsume implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;        // 用户ID
    private double totalAmount;   // 累计消费金额
    private long updateTime;      // 更新时间

    public UserTotalConsume() {}

    public UserTotalConsume(String userId, double totalAmount, long updateTime) {
        this.userId = userId;
        this.totalAmount = totalAmount;
        this.updateTime = updateTime;
    }

    // getter和setter方法
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "UserTotalConsume{" +
                "userId='" + userId + '\'' +
                ", totalAmount=" + totalAmount +
                ", updateTime=" + updateTime +
                '}';
    }
}