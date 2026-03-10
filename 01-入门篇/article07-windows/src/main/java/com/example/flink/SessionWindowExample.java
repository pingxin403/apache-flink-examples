package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Session Window（会话窗口）示例
 * 
 * 功能：分析用户会话行为
 * 
 * 窗口特点：
 * - 窗口大小不固定，基于活动间隔（Gap）
 * - 如果两个事件之间的间隔超过Gap，就划分为两个窗口
 * - 适合分析用户会话、设备活跃时段等场景
 * 
 * 本示例：
 * - Gap设置为5秒
 * - 如果用户5秒内没有操作，就认为会话结束
 */
public class SessionWindowExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. 创建用户行为数据流
        DataStream<UserAction> actions = env
            .addSource(new UserActionSource())
            .name("User Action Source");
        
        // 3. 分配时间戳和 Watermark
        DataStream<UserAction> actionsWithTimestamp = actions
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserAction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((action, timestamp) -> action.getTimestamp())
            );
        
        // 4. 使用 Session Window 分析用户会话
        // Gap设置为5秒：如果用户5秒内没有操作，就认为会话结束
        DataStream<Tuple3<String, Long, String>> result = actionsWithTimestamp
            .keyBy(UserAction::getUserId)
            .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
            .process(new SessionAnalysisFunction());
        
        // 5. 打印结果
        result.print("Session Window Result");
        
        // 6. 执行作业
        env.execute("Session Window Example");
    }
    
    /**
     * 会话分析函数
     * 
     * 输出格式：(用户ID, 会话中的操作次数, 会话时长)
     */
    public static class SessionAnalysisFunction 
            extends ProcessWindowFunction<UserAction, Tuple3<String, Long, String>, String, TimeWindow> {
        
        private static final DateTimeFormatter formatter = 
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
        
        @Override
        public void process(String userId, 
                          Context context, 
                          Iterable<UserAction> elements, 
                          Collector<Tuple3<String, Long, String>> out) {
            // 统计会话中的操作次数
            long count = 0;
            for (UserAction action : elements) {
                count++;
            }
            
            // 获取窗口信息
            TimeWindow window = context.window();
            long sessionStart = window.getStart();
            long sessionEnd = window.getEnd();
            long sessionDuration = sessionEnd - sessionStart;
            
            // 格式化会话信息
            String sessionInfo = String.format(
                "操作次数=%d, 会话时长=%d秒, 开始时间=%s, 结束时间=%s",
                count,
                sessionDuration / 1000,
                formatter.format(Instant.ofEpochMilli(sessionStart)),
                formatter.format(Instant.ofEpochMilli(sessionEnd))
            );
            
            out.collect(Tuple3.of(userId, count, sessionInfo));
        }
    }
}
