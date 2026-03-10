package com.example.flink;

import com.example.flink.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * 空闲源处理示例
 * 
 * 演示如何使用 withIdleness() 处理空闲数据源
 * 
 * 核心要点:
 * 1. 多分区数据源中,某些分区可能长时间无数据
 * 2. 空闲分区的 Watermark 不推进,会拖累整个作业
 * 3. 使用 withIdleness() 标记空闲源,避免影响 Watermark 推进
 */
public class IdleSourceExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3); // 3 个并行度,模拟 3 个分区

        // 创建数据源:模拟多分区数据,其中部分分区空闲
        DataStream<Event> events = env.addSource(new MultiPartitionSource());

        // 配置 Watermark 策略 + 空闲超时
        DataStream<Event> withWatermarks = events.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 关键配置:10 秒无数据则标记为空闲
                .withIdleness(Duration.ofSeconds(10))
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

        env.execute("Idle Source Example");
    }

    /**
     * 多分区数据源
     * 
     * 模拟 Kafka 多分区场景:
     * - 分区 0:流量正常,持续产生数据
     * - 分区 1:流量正常,持续产生数据
     * - 分区 2:空闲,长时间无数据
     */
    private static class MultiPartitionSource extends RichParallelSourceFunction<Event> {
        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
            long baseTime = System.currentTimeMillis();
            int eventCount = 0;

            System.out.println("分区 " + partitionIndex + " 启动");

            while (running && eventCount < 100) {
                // 分区 2 模拟空闲:只在前 10 个事件后就停止产生数据
                if (partitionIndex == 2 && eventCount >= 10) {
                    System.out.println("分区 2 进入空闲状态...");
                    Thread.sleep(1000);
                    continue;
                }

                // 生成事件时间戳
                long eventTime = baseTime + eventCount * 1000;

                // 生成事件
                String userId = "user_" + (random.nextInt(3) + 1);
                String eventType = "click";
                Event event = new Event(userId, eventType, eventTime);

                System.out.println("分区 " + partitionIndex + " 生成事件: " + event);
                ctx.collect(event);

                eventCount++;
                Thread.sleep(500); // 模拟数据到达间隔
            }

            System.out.println("分区 " + partitionIndex + " 结束");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
