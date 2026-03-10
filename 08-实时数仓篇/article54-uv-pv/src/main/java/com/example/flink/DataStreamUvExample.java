package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * DataStream API 实现实时 UV 统计示例
 *
 * 演示内容：
 * 1. 使用 ProcessWindowFunction + HashSet 精确统计 UV
 * 2. 使用自定义 Source 模拟页面访问数据
 * 3. 同时输出 PV 和 UV，展示去重效果
 *
 * 三种 UV 去重方案对比：
 *   - 本示例：HashSet 精确去重（适合日活 < 100 万）
 *   - HyperLogLog：近似去重，内存极小（适合日活千万+）
 *   - RoaringBitmap：整数 ID 精确去重（适合日活 < 1 亿）
 *
 * 注意事项：
 * - HashSet 方案的内存与窗口内用户数成正比
 * - 生产环境大规模场景建议使用 HyperLogLog
 * - 本示例使用自定义 Source 模拟数据，生产环境替换为 Kafka Source
 *
 * @author 韩云朋
 * @see <a href="https://github.com/pingxin403/apache-flink-examples">GitHub</a>
 */
public class DataStreamUvExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ========== 第一步：创建模拟数据源 ==========
        // 模拟页面访问事件：(user_id, page_id, event_time_ms)
        DataStream<PageViewEvent> source = env
                .addSource(new PageViewSource())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<PageViewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.eventTime)
                );

        // ========== 第二步：按页面分组，统计每分钟 PV 和 UV ==========
        DataStream<String> pvUvResult = source
                .keyBy(event -> event.pageId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new PvUvProcessFunction());

        // ========== 第三步：输出结果 ==========
        pvUvResult.print("PV/UV");

        env.execute("DataStream UV Example");
    }

    // ========== 页面访问事件 POJO ==========
    public static class PageViewEvent {
        public int userId;
        public String pageId;
        public long eventTime;

        public PageViewEvent() {}

        public PageViewEvent(int userId, String pageId, long eventTime) {
            this.userId = userId;
            this.pageId = pageId;
            this.eventTime = eventTime;
        }
    }

    // ========== 模拟数据源 ==========
    public static class PageViewSource extends RichSourceFunction<PageViewEvent> {
        private volatile boolean running = true;
        private final Random random = new Random();
        // 模拟 5 个页面
        private final String[] pages = {"home", "product", "cart", "order", "pay"};

        @Override
        public void run(SourceContext<PageViewEvent> ctx) throws Exception {
            while (running) {
                // 模拟 50 个用户随机访问页面
                int userId = random.nextInt(50) + 1;
                String pageId = pages[random.nextInt(pages.length)];
                long eventTime = System.currentTimeMillis();

                ctx.collect(new PageViewEvent(userId, pageId, eventTime));
                // 每 100ms 产生一条数据
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ========== PV/UV 统计窗口函数 ==========
    /**
     * 使用 HashSet 精确去重统计 UV
     * 同时统计 PV（总访问次数）
     *
     * 优点：结果 100% 精确
     * 缺点：内存与用户数成正比，大规模场景不适用
     */
    public static class PvUvProcessFunction
            extends ProcessWindowFunction<PageViewEvent, String, String, TimeWindow> {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("HH:mm:ss");

        @Override
        public void process(String pageId, Context ctx,
                            Iterable<PageViewEvent> elements,
                            Collector<String> out) {
            // 用 HashSet 收集窗口内所有 user_id，实现精确去重
            Set<Integer> userSet = new HashSet<>();
            long pvCount = 0;

            for (PageViewEvent event : elements) {
                userSet.add(event.userId);
                pvCount++;
            }

            // UV = HashSet 的大小（去重后的用户数）
            long uvCount = userSet.size();

            // 格式化窗口时间
            String windowStart = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(ctx.window().getStart()),
                    ZoneId.systemDefault()).format(FMT);
            String windowEnd = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(ctx.window().getEnd()),
                    ZoneId.systemDefault()).format(FMT);

            out.collect(String.format(
                    "页面=%s | 窗口=[%s, %s) | PV=%d | UV=%d",
                    pageId, windowStart, windowEnd, pvCount, uvCount));
        }
    }
}
