package com.example.flink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 用户会话管理示例
 * 演示如何使用 TTL 管理用户会话，30 分钟无活动自动过期
 */
public class SessionManager extends KeyedProcessFunction<String, UserEvent, SessionAlert> {
    
    private ValueState<UserSession> sessionState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 配置 TTL：30 分钟
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.minutes(30))
            // 任何活动（读或写）都重置 TTL
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            // 过期状态永不返回
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            // 启用增量清理：每次访问清理 5 个过期条目
            .cleanupIncrementally(5, true)
            .build();
        
        ValueStateDescriptor<UserSession> descriptor = 
            new ValueStateDescriptor<>("session", UserSession.class);
        descriptor.enableTimeToLive(ttlConfig);
        
        sessionState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(UserEvent event, Context ctx, Collector<SessionAlert> out) 
            throws Exception {
        UserSession session = sessionState.value();
        
        if (session == null) {
            // 新会话开始
            session = new UserSession(event.getUserId(), System.currentTimeMillis());
            out.collect(new SessionAlert(
                event.getUserId(), 
                "会话开始", 
                System.currentTimeMillis()
            ));
        }
        
        // 更新会话（自动重置 TTL）
        session.setLastActivityTime(System.currentTimeMillis());
        session.incrementEventCount();
        sessionState.update(session);
        
        // 输出会话信息
        out.collect(new SessionAlert(
            event.getUserId(),
            "会话活动: " + session.getEventCount() + " 个事件",
            System.currentTimeMillis()
        ));
    }
}
