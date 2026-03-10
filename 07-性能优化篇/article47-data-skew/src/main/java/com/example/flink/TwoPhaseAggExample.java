package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Local-Global 两阶段聚合示例
 *
 * 演示如何使用 AggregateFunction 实现增量聚合，配合 ProcessWindowFunction
 * 获取窗口元信息，实现高效的两阶段聚合。
 *
 * 核心思想：
 *   - AggregateFunction 在每个 subtask 内部做增量预聚合（Local 阶段）
 *   - 每条数据到来时只更新累加器，不缓存原始数据
 *   - 窗口触发时输出聚合结果（Global 阶段由框架的 keyBy 保证）
 *
 * 与 ReduceFunction 相比，AggregateFunction 支持不同的输入/累加器/输出类型，
 * 更加灵活，适合复杂的聚合逻辑。
 */
public class TwoPhaseAggExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 模拟订单数据流：Tuple2<商品ID, 订单金额>
        DataStream<Tuple2<String, Long>> source = env.fromElements(
                Tuple2.of("productA", 100L),
                Tuple2.of("productA", 200L),
                Tuple2.of("productA", 150L),
                Tuple2.of("productA", 300L),
                Tuple2.of("productA", 250L),
                Tuple2.of("productB", 50L),
                Tuple2.of("productB", 80L),
                Tuple2.of("productC", 60L),
                Tuple2.of("productC", 90L),
                Tuple2.of("productD", 70L)
        );

        // 使用 AggregateFunction + ProcessWindowFunction 实现两阶段聚合
        // AggregateFunction 负责增量预聚合（Local 阶段）
        // ProcessWindowFunction 负责输出最终结果并附加窗口信息（Global 阶段）
        DataStream<String> result = source
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new OrderAggregateFunction(),
                        new OrderProcessWindowFunction()
                );

        result.print("聚合结果");

        env.execute("Two-Phase Aggregation Example");
    }

    /**
     * 累加器：同时统计订单数和总金额
     */
    public static class OrderAccumulator {
        long count = 0;
        long totalAmount = 0;

        @Override
        public String toString() {
            return "OrderAccumulator{count=" + count + ", totalAmount=" + totalAmount + "}";
        }
    }

    /**
     * 增量聚合函数（Local 阶段）
     *
     * 输入：Tuple2<商品ID, 订单金额>
     * 累加器：OrderAccumulator（订单数 + 总金额）
     * 输出：OrderAccumulator（传递给 ProcessWindowFunction）
     *
     * 每条数据到来时只更新累加器中的 count 和 totalAmount，
     * 不缓存任何原始数据，内存开销极低。
     */
    public static class OrderAggregateFunction
            implements AggregateFunction<Tuple2<String, Long>, OrderAccumulator, OrderAccumulator> {

        @Override
        public OrderAccumulator createAccumulator() {
            return new OrderAccumulator();
        }

        @Override
        public OrderAccumulator add(Tuple2<String, Long> value, OrderAccumulator acc) {
            acc.count += 1;
            acc.totalAmount += value.f1;
            return acc;
        }

        @Override
        public OrderAccumulator getResult(OrderAccumulator acc) {
            return acc;
        }

        @Override
        public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
            // 合并两个累加器（Session 窗口合并时调用）
            a.count += b.count;
            a.totalAmount += b.totalAmount;
            return a;
        }
    }

    /**
     * 窗口处理函数（Global 阶段）
     *
     * 接收 AggregateFunction 的输出结果，附加窗口元信息后输出最终结果。
     * 此时每个 Key 每个窗口只有一条聚合结果，处理开销极低。
     */
    public static class OrderProcessWindowFunction
            extends ProcessWindowFunction<OrderAccumulator, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            ProcessWindowFunction<OrderAccumulator, String, String, TimeWindow>.Context context,
                            Iterable<OrderAccumulator> elements,
                            Collector<String> out) {

            OrderAccumulator acc = elements.iterator().next();
            TimeWindow window = context.window();

            String output = String.format(
                    "商品: %s | 窗口: [%d, %d) | 订单数: %d | 总金额: %d | 平均金额: %.2f",
                    key,
                    window.getStart(),
                    window.getEnd(),
                    acc.count,
                    acc.totalAmount,
                    acc.count > 0 ? (double) acc.totalAmount / acc.count : 0.0
            );
            out.collect(output);
        }
    }
}
