package com.example.flink;

import com.example.flink.model.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 标点式 Watermark 示例
 * 
 * 演示如何根据数据内容动态生成 Watermark
 * 
 * 核心要点:
 * 1. 实现 WatermarkGenerator 接口
 * 2. 在 onEvent() 方法中判断是否生成 Watermark
 * 3. 适合数据流中有明确"时间边界"标记的场景
 */
public class PunctuatedWatermarkExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建数据源:模拟带有 Watermark 标记的数据
        DataStream<Event> events = env.addSource(new EventSourceWithMarkers());

        // 配置标点式 Watermark 策略
        DataStream<Event> withWatermarks = events.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forGenerator(ctx -> new PunctuatedWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        // 使用事件时间窗口:10 秒滚动窗口
        withWatermarks
            .filter(event -> !event.isWatermarkMarker()) // 过滤掉 Watermark 标记事件
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

        env.execute("Punctuated Watermark Example");
    }

    /**
     * 标点式 Watermark 生成器
     * 
     * 根据事件内容判断是否生成 Watermark:
     * - 如果事件包含 watermarkMarker 标记,立即生成 Watermark
     * - Watermark 时间戳为事件的时间戳
     */
    private static class PunctuatedWatermarkGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 如果事件包含 Watermark 标记,立即生成 Watermark
            if (event.isWatermarkMarker()) {
                System.out.println("生成标点式 Watermark: " + event.getTimestamp());
                output.emitWatermark(new Watermark(event.getTimestamp()));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 标点式 Watermark 不需要周期性生成
            // 这个方法可以留空
        }
    }

    /**
     * 自定义数据源:生成带有 Watermark 标记的事件
     * 
     * 模拟批流混合场景:
     * - 每 10 个事件为一批
     * - 每批的最后一个事件标记为 Watermark 标记
     */
    private static class EventSourceWithMarkers implements SourceFunction<Event> {
        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();
            int eventCount = 0;
            int batchSize = 10;

            while (running && eventCount < 50) {
                // 生成事件时间戳
                long eventTime = baseTime + eventCount * 1000;

                // 生成事件
                String userId = "user_" + (random.nextInt(3) + 1);
                String eventType = "click";

                // 判断是否为批次的最后一个事件
                boolean isMarker = (eventCount + 1) % batchSize == 0;
                Event event = new Event(userId, eventType, eventTime, isMarker);

                if (isMarker) {
                    System.out.println("生成 Watermark 标记事件: " + event);
                } else {
                    System.out.println("生成普通事件: " + event);
                }

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
