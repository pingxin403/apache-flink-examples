package com.example.flink;

import java.io.Serializable;

/**
 * 商品销量
 */
public class ProductSales implements Serializable {
    private String productId;
    private long salesCount;
    private String period;
    
    public ProductSales() {}
    
    public ProductSales(String productId, long salesCount, String period) {
        this.productId = productId;
        this.salesCount = salesCount;
        this.period = period;
    }
    
    public String getProductId() {
        return productId;
    }
    
    public void setProductId(String productId) {
        this.productId = productId;
    }
    
    public long getSalesCount() {
        return salesCount;
    }
    
    public void setSalesCount(long salesCount) {
        this.salesCount = salesCount;
    }
    
    public String getPeriod() {
        return period;
    }
    
    public void setPeriod(String period) {
        this.period = period;
    }
    
    @Override
    public String toString() {
        return "ProductSales{" +
                "productId='" + productId + '\'' +
                ", salesCount=" + salesCount +
                ", period='" + period + '\'' +
                '}';
    }
}
