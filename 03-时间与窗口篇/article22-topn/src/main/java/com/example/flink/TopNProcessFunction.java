package com.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * TOP-N 处理函数
 * 使用定时器机制,在窗口结束后对所有商品销量进行排序,输出 TOP-N
 */
public class TopNProcessFunction 
    extends KeyedProcessFunction<String, ProductSales, String> {

    private final int topN;
    private ListState<ProductSales> salesState;

    public TopNProcessFunction(int topN) {
        this.topN = topN;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态,用于存储同一窗口内所有商品的销量
        ListStateDescriptor<ProductSales> descriptor = 
            new ListStateDescriptor<>("sales-state", ProductSales.class);
        salesState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(ProductSales value,
                              Context ctx,
                              Collector<String> out) throws Exception {
        // 将当前商品销量加入状态
        salesState.add(value);
        
        // 注册定时器,在窗口结束时触发排序
        // 加1是为了确保所有商品的销量都已到达
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp,
                       OnTimerContext ctx,
                       Collector<String> out) throws Exception {
        // 从状态中取出所有商品销量
        List<ProductSales> allSales = new ArrayList<>();
        for (ProductSales sales : salesState.get()) {
            allSales.add(sales);
        }
        
        // 清空状态,避免内存泄漏
        salesState.clear();
        
        // 按销量降序排序
        allSales.sort((a, b) -> 
            b.getTotalQuantity().compareTo(a.getTotalQuantity()));
        
        // 构造输出结果
        StringBuilder result = new StringBuilder();
        result.append("\n========================================\n");
        result.append("           TOP ").append(topN).append(" 商品销量排行榜\n");
        result.append("========================================\n");
        result.append("窗口结束时间: ")
              .append(new Timestamp(timestamp - 1)).append("\n");
        result.append("----------------------------------------\n");
        
        // 输出 TOP-N
        int rank = 1;
        for (int i = 0; i < Math.min(topN, allSales.size()); i++) {
            ProductSales sales = allSales.get(i);
            result.append(String.format("NO.%-2d | %-10s | 销量: %-6d\n",
                rank++,
                sales.getProductId(),
                sales.getTotalQuantity()));
        }
        
        result.append("========================================\n");
        
        out.collect(result.toString());
    }
}
