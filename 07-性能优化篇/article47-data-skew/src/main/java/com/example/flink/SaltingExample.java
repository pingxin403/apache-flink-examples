package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Key Salting（加盐打散）示例
 *
 * 演示如何通过给热点 Key 添加随机后缀，将数据均匀分散到多个 subtask，
 * 然后通过两阶段聚合得到最终结果。
 *
 * 场景：电商订单按商品 ID 统计销量，某些热门商品（如 productA）的订单量
 * 远超其他商品，导致 keyBy(productId) 后数据严重倾斜。
 *
 * 思路：
 *   阶段一：给 Key 加盐（productA → productA_0, productA_1, ...），分散聚合
 *   阶段二：去盐恢复原始 Key，做最终聚合
 */
public class SaltingExample {

    /** 盐值数量，建议设置为并行度的 2-4 倍 */
    private static final int SALT_FACTOR = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 模拟带有热点 Key 的订单数据流
        // Tuple2<商品ID, 订单金额>
        DataStream<Tuple2<String, Long>> source = env.fromElements(
                // 热门商品 productA：大量订单
                Tuple2.of("productA", 100L),
                Tuple2.of("productA", 200L),
                Tuple2.of("productA", 150L),
                Tuple2.of("productA", 300L),
                Tuple2.of("productA", 250L),
                Tuple2.of("productA", 180L),
                Tuple2.of("productA", 120L),
                Tuple2.of("productA", 350L),
                // 普通商品：少量订单
                Tuple2.of("productB", 50L),
                Tuple2.of("productC", 80L),
                Tuple2.of("productD", 60L),
                Tuple2.of("productE", 90L)
        );

        // ========== 阶段一：加盐 + 局部聚合 ==========
        DataStream<Tuple2<String, Long>> saltedResult = source
                // 给 Key 加盐：原始 Key + "_" + 随机数
                .map(new SaltKeyMapper(SALT_FACTOR))
                // 按加盐后的 Key 分组
                .keyBy(t -> t.f0)
                // 窗口聚合（局部结果）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new SumReducer());

        // ========== 阶段二：去盐 + 全局聚合 ==========
        DataStream<Tuple2<String, Long>> finalResult = saltedResult
                // 去盐：去掉 "_数字" 后缀，恢复原始 Key
                .map(new RemoveSaltMapper())
                // 按原始 Key 分组
                .keyBy(t -> t.f0)
                // 窗口聚合（最终结果）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new SumReducer());

        finalResult.print("最终结果");

        env.execute("Key Salting Example");
    }

    /**
     * 加盐 Mapper：给 Key 添加随机后缀
     * 例如：productA → productA_3
     */
    public static class SaltKeyMapper
            implements MapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        private final int saltFactor;

        public SaltKeyMapper(int saltFactor) {
            this.saltFactor = saltFactor;
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> value) {
            int salt = ThreadLocalRandom.current().nextInt(saltFactor);
            String saltedKey = value.f0 + "_" + salt;
            return Tuple2.of(saltedKey, value.f1);
        }
    }

    /**
     * 去盐 Mapper：去掉 Key 的随机后缀，恢复原始 Key
     * 例如：productA_3 → productA
     */
    public static class RemoveSaltMapper
            implements MapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> value) {
            String originalKey = value.f0.substring(0, value.f0.lastIndexOf("_"));
            return Tuple2.of(originalKey, value.f1);
        }
    }

    /**
     * 求和 Reducer：对相同 Key 的金额求和
     */
    public static class SumReducer
            implements ReduceFunction<Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return Tuple2.of(a.f0, a.f1 + b.f1);
        }
    }
}
