package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 会话增量聚合函数
 * 每个元素到达时更新累加器,避免全量遍历
 */
public class SessionAggregateFunction 
    implements AggregateFunction<UserAction, SessionAccumulator, SessionAccumulator> {
    
    @Override
    public SessionAccumulator createAccumulator() {
        return new SessionAccumulator();
    }
    
    @Override
    public SessionAccumulator add(UserAction action, SessionAccumulator acc) {
        // 更新会话开始和结束时间
        if (acc.sessionStart == null || action.getTimestamp() < acc.sessionStart) {
            acc.sessionStart = action.getTimestamp();
        }
        if (acc.sessionEnd == null || action.getTimestamp() > acc.sessionEnd) {
            acc.sessionEnd = action.getTimestamp();
        }
        
        // 统计不同类型的行为
        switch (action.getActionType()) {
            case "VIEW":
                acc.viewCount++;
                acc.viewPath.add(action.getProductId());
                break;
            case "ADD_CART":
                acc.addCartCount++;
                break;
            case "ORDER":
                acc.orderCount++;
                acc.totalAmount += action.getPrice();
                break;
        }
        
        return acc;
    }
    
    @Override
    public SessionAccumulator getResult(SessionAccumulator acc) {
        return acc;
    }
    
    @Override
    public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
        // 合并两个会话的统计数据
        // 这个方法在会话窗口合并时被调用
        SessionAccumulator merged = new SessionAccumulator();
        merged.sessionStart = Math.min(a.sessionStart, b.sessionStart);
        merged.sessionEnd = Math.max(a.sessionEnd, b.sessionEnd);
        merged.viewCount = a.viewCount + b.viewCount;
        merged.addCartCount = a.addCartCount + b.addCartCount;
        merged.orderCount = a.orderCount + b.orderCount;
        merged.totalAmount = a.totalAmount + b.totalAmount;
        merged.viewPath.addAll(a.viewPath);
        merged.viewPath.addAll(b.viewPath);
        return merged;
    }
}
