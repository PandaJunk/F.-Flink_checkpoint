package com.example.flink.model;

import java.io.Serializable;

/**
 * 商品销售额输出模型
 */
public class ProductSales implements Serializable {
    private static final long serialVersionUID = 1L;

    private String subtaskId;      // 并行子任务ID（用于定位故障节点）
    private long orderCount;       // 订单数
    private double totalSales;     // 总销售额
    private long windowEndTime;    // 窗口结束时间

    public ProductSales() {}

    public ProductSales(String subtaskId, long orderCount, double totalSales, long windowEndTime) {
        this.subtaskId = subtaskId;
        this.orderCount = orderCount;
        this.totalSales = totalSales;
        this.windowEndTime = windowEndTime;
    }

    // getter和setter方法
    public String getSubtaskId() {
        return subtaskId;
    }

    public void setSubtaskId(String subtaskId) {
        this.subtaskId = subtaskId;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public double getTotalSales() {
        return totalSales;
    }

    public void setTotalSales(double totalSales) {
        this.totalSales = totalSales;
    }

    public long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ProductSales{" +
                "subtaskId='" + subtaskId + '\'' +
                ", orderCount=" + orderCount +
                ", totalSales=" + totalSales +
                ", windowEndTime=" + windowEndTime +
                '}';
    }
}