package com.example.flink;

import java.io.Serializable;

/**
 * 商品销量统计模型
 */
public class ProductSales implements Serializable {
    private static final long serialVersionUID = 1L;

    private String productId;      // 商品ID
    private String productName;    // 商品名称
    private Long totalQuantity;    // 总销量
    private Long windowEnd;        // 窗口结束时间

    public ProductSales() {
    }

    public ProductSales(String productId, String productName, 
                       Long totalQuantity, Long windowEnd) {
        this.productId = productId;
        this.productName = productName;
        this.totalQuantity = totalQuantity;
        this.windowEnd = windowEnd;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(Long totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ProductSales{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", totalQuantity=" + totalQuantity +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
