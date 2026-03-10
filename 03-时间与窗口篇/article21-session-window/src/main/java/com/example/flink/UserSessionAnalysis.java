package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 用户会话分析主程序
 * 
 * 功能:
 * 1. 使用会话窗口识别用户的购物会话
 * 2. 统计每次会话的行为路径(浏览→加购→下单)
 * 3. 计算会话的转化率
 * 
 * 运行方式:
 * mvn clean package
 * java -cp target/article21-session-window-1.0-SNAPSHOT.jar com.example.flink.UserSessionAnalysis
 */
public class UserSessionAnalysis {
    
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度为 1,方便观察输出
        env.setParallelism(1);
        
        // 读取用户行为数据
        DataStream<UserAction> actions = env
            .addSource(new UserActionSource())
            .name("User Action Source");
        
        // 设置 Watermark 策略
        // 允许 10 秒的乱序,1 分钟没有数据则标记为空闲
        DataStream<UserAction> actionsWithWatermark = actions
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserAction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                    .withIdleness(Duration.ofMinutes(1))
            );
        
        // 使用会话窗口分析用户行为
        // Gap 设置为 10 分钟:如果用户 10 分钟内没有操作,认为会话结束
        DataStream<SessionStats> sessionStats = actionsWithWatermark
            .keyBy(UserAction::getUserId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
            .aggregate(
                new SessionAggregateFunction(),      // 增量聚合
                new SessionProcessWindowFunction()   // 窗口处理
            );
        
        // 输出结果
        sessionStats.print();
        
        // 执行作业
        env.execute("User Session Analysis");
    }
}
