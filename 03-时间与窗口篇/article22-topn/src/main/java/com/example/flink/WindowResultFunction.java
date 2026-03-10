package com.example.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口结果处理函数
 * 将聚合结果附加窗口信息和商品信息
 */
public class WindowResultFunction 
    extends ProcessWindowFunction<Long, ProductSales, String, TimeWindow> {

    @Override
    public void process(String productId,
                       Context context,
                       Iterable<Long> elements,
                       Collector<ProductSales> out) {
        // 获取聚合结果(销量总数)
        Long totalQuantity = elements.iterator().next();
        
        // 获取窗口结束时间
        Long windowEnd = context.window().getEnd();
        
        // 构造商品销量统计对象
        ProductSales sales = new ProductSales();
        sales.setProductId(productId);
        sales.setProductName("商品-" + productId);  // 实际场景中应该从维表查询
        sales.setTotalQuantity(totalQuantity);
        sales.setWindowEnd(windowEnd);
        
        // 输出结果
        out.collect(sales);
    }
}
