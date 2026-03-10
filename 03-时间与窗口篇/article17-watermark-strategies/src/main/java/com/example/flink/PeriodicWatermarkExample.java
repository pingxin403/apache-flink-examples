package com.example.flink;

import com.example.flink.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * 周期性 Watermark 示例
 * 
 * 演示如何使用周期性 Watermark 处理乱序数据
 * 
 * 核心要点:
 * 1. 使用 forBoundedOutOfOrderness() 指定最大乱序时间
 * 2. Flink 每 200ms 自动生成一次 Watermark
 * 3. Watermark = 当前最大事件时间戳 - 最大乱序时间
 */
public class PeriodicWatermarkExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置 Watermark 生成间隔为 1 秒(默认 200ms)
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 创建数据源:模拟乱序数据
        DataStream<Event> events = env.addSource(new OutOfOrderEventSource());

        // 配置周期性 Watermark 策略
        DataStream<Event> withWatermarks = events.assignTimestampsAndWatermarks(
            WatermarkStrategy
                // 允许 5 秒的乱序
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 从事件中提取时间戳
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        // 使用事件时间窗口:10 秒滚动窗口
        withWatermarks
            .keyBy(Event::getUserId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context context,
                                    Iterable<Event> elements,
                                    Collector<String> out) {
                    long count = 0;
                    for (Event event : elements) {
                        count++;
                    }

                    long windowStart = context.window().getStart();
                    long windowEnd = context.window().getEnd();

                    String result = String.format(
                        "窗口 [%d - %d] 用户 %s 的事件数: %d",
                        windowStart, windowEnd, key, count
                    );
                    out.collect(result);
                }
            })
            .print();

        env.execute("Periodic Watermark Example");
    }

    /**
     * 自定义数据源:生成乱序事件
     * 
     * 模拟真实场景中的数据乱序情况:
     * - 大部分数据按时间顺序到达
     * - 偶尔有延迟数据(延迟 1-5 秒)
     */
    private static class OutOfOrderEventSource implements SourceFunction<Event> {
        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();
            int eventCount = 0;

            while (running && eventCount < 100) {
                // 生成事件时间戳
                long eventTime;
                if (random.nextDouble() < 0.8) {
                    // 80% 的数据按顺序到达
                    eventTime = baseTime + eventCount * 1000;
                } else {
                    // 20% 的数据延迟 1-5 秒
                    int delay = random.nextInt(5) + 1;
                    eventTime = baseTime + (eventCount - delay) * 1000;
                }

                // 生成事件
                String userId = "user_" + (random.nextInt(3) + 1);
                String eventType = "click";
                Event event = new Event(userId, eventType, eventTime);

                System.out.println("生成事件: " + event);
                ctx.collect(event);

                eventCount++;
                Thread.sleep(100); // 模拟数据到达间隔
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
