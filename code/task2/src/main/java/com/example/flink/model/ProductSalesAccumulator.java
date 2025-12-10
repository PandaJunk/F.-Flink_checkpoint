package com.example.flink.model;

import java.io.Serializable;

/**
 * 商品销售额累加器（窗口聚合用）
 */
public class ProductSalesAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private long orderCount;   // 订单数
    private double totalSales; // 总销售额

    public ProductSalesAccumulator() {}

    public ProductSalesAccumulator(long orderCount, double totalSales) {
        this.orderCount = orderCount;
        this.totalSales = totalSales;
    }

    // getter和setter方法
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

    @Override
    public String toString() {
        return "ProductSalesAccumulator{" +
                "orderCount=" + orderCount +
                ", totalSales=" + totalSales +
                '}';
    }
}