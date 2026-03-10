package com.example.flink;

import com.example.flink.model.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * ValueState 示例：监控用户频繁登录
 * 
 * 功能：记录每个用户的最新登录时间，如果1小时内重复登录则告警
 */
public class ValueStateExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟登录事件流
        DataStream<LoginEvent> loginStream = env
                .addSource(new LoginEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        // 按用户ID分组，检测频繁登录
        DataStream<String> alerts = loginStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginMonitor());

        // 输出告警
        alerts.print();

        env.execute("ValueState Example - Login Monitor");
    }

    /**
     * 登录监控函数
     * 使用 ValueState 记录用户最新登录时间
     */
    public static class LoginMonitor extends KeyedProcessFunction<String, LoginEvent, String> {

        // 状态：记录最新登录时间
        private ValueState<Long> lastLoginTimeState;

        @Override
        public void open(Configuration parameters) {
            // 初始化状态描述符
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("lastLoginTime", Long.class);
            lastLoginTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(LoginEvent event, Context ctx, Collector<String> out)
                throws Exception {
            // 读取状态
            Long lastLoginTime = lastLoginTimeState.value();
            long currentTime = event.getTimestamp();

            if (lastLoginTime != null) {
                long timeDiff = currentTime - lastLoginTime;

                // 1小时内重复登录（3600000毫秒）
                if (timeDiff < 60 * 60 * 1000) {
                    String alert = String.format(
                            "⚠️ 用户 %s 频繁登录告警：间隔仅 %d 秒 (IP: %s)",
                            event.getUserId(),
                            timeDiff / 1000,
                            event.getIp()
                    );
                    out.collect(alert);
                }
            }

            // 更新状态
            lastLoginTimeState.update(currentTime);
        }
    }

    /**
     * 模拟登录事件源
     */
    public static class LoginEventSource implements SourceFunction<LoginEvent> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"user1", "user2", "user3"};
        private final String[] ips = {"192.168.1.1", "192.168.1.2", "192.168.1.3"};

        @Override
        public void run(SourceContext<LoginEvent> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();

            while (running) {
                String userId = users[random.nextInt(users.length)];
                String ip = ips[random.nextInt(ips.length)];
                long timestamp = baseTime;

                LoginEvent event = new LoginEvent(userId, timestamp, ip);
                ctx.collect(event);

                // 模拟频繁登录：30%概率在短时间内再次登录
                if (random.nextDouble() < 0.3) {
                    Thread.sleep(random.nextInt(30 * 60 * 1000)); // 0-30分钟
                } else {
                    Thread.sleep(random.nextInt(2 * 60 * 60 * 1000)); // 0-2小时
                }

                baseTime = System.currentTimeMillis();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
