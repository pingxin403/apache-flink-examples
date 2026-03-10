package com.example.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 会话窗口处理函数
 * 将累加器转换为最终的会话统计结果
 */
public class SessionProcessWindowFunction 
    extends ProcessWindowFunction<SessionAccumulator, SessionStats, String, TimeWindow> {
    
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    @Override
    public void process(String userId, Context context, 
                       Iterable<SessionAccumulator> elements, 
                       Collector<SessionStats> out) {
        SessionAccumulator acc = elements.iterator().next();
        
        SessionStats stats = new SessionStats();
        stats.setUserId(userId);
        stats.setSessionStart(acc.sessionStart);
        stats.setSessionEnd(acc.sessionEnd);
        stats.setViewCount(acc.viewCount);
        stats.setAddCartCount(acc.addCartCount);
        stats.setOrderCount(acc.orderCount);
        stats.setTotalAmount(acc.totalAmount);
        stats.setViewPath(acc.viewPath);
        
        // 计算会话时长(秒)
        long duration = (acc.sessionEnd - acc.sessionStart) / 1000;
        stats.setDuration(duration);
        
        // 计算转化率
        if (acc.viewCount > 0) {
            stats.setAddCartRate((double) acc.addCartCount / acc.viewCount);
            stats.setOrderRate((double) acc.orderCount / acc.viewCount);
        } else {
            stats.setAddCartRate(0.0);
            stats.setOrderRate(0.0);
        }
        
        // 打印会话信息
        System.out.println("=== 会话结束 ===");
        System.out.println("用户: " + userId);
        System.out.println("开始时间: " + sdf.format(new Date(acc.sessionStart)));
        System.out.println("结束时间: " + sdf.format(new Date(acc.sessionEnd)));
        System.out.println("时长: " + duration + " 秒");
        System.out.println("浏览: " + acc.viewCount + " 次");
        System.out.println("加购: " + acc.addCartCount + " 次");
        System.out.println("下单: " + acc.orderCount + " 次");
        System.out.println("金额: " + String.format("%.2f", acc.totalAmount) + " 元");
        System.out.println("加购率: " + String.format("%.2f%%", stats.getAddCartRate() * 100));
        System.out.println("下单率: " + String.format("%.2f%%", stats.getOrderRate() * 100));
        System.out.println("浏览路径: " + acc.viewPath);
        System.out.println("================\n");
        
        out.collect(stats);
    }
}
