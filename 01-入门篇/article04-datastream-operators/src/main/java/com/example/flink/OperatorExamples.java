package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream 常用算子示例
 * 演示 map、filter、flatMap、keyBy 等算子的使用
 */
public class OperatorExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 示例 1：map 算子 - 一对一转换
        System.out.println("=== 示例 1：map 算子 ===");
        mapExample(env);

        // 示例 2：filter 算子 - 数据过滤
        System.out.println("\n=== 示例 2：filter 算子 ===");
        filterExample(env);

        // 示例 3：flatMap 算子 - 一对多转换
        System.out.println("\n=== 示例 3：flatMap 算子 ===");
        flatMapExample(env);

        // 示例 4：keyBy + sum 算子 - 分组聚合
        System.out.println("\n=== 示例 4：keyBy + sum 算子 ===");
        keyByExample(env);

        // 执行作业
        env.execute("Operator Examples");
    }

    /**
     * 示例 1：map 算子
     * 功能：将字符串转换为大写
     */
    private static void mapExample(StreamExecutionEnvironment env) {
        DataStream<String> input = env.fromElements("hello", "world", "flink");
        
        // 使用 map 将字符串转换为大写
        DataStream<String> result = input.map(String::toUpperCase);
        
        result.print("Map Result");
    }

    /**
     * 示例 2：filter 算子
     * 功能：过滤出长度大于 5 的字符串
     */
    private static void filterExample(StreamExecutionEnvironment env) {
        DataStream<String> input = env.fromElements("hello", "world", "flink", "streaming", "api");
        
        // 使用 filter 过滤出长度大于 5 的字符串
        DataStream<String> result = input.filter(s -> s.length() > 5);
        
        result.print("Filter Result");
    }

    /**
     * 示例 3：flatMap 算子
     * 功能：将句子拆分为单词
     */
    private static void flatMapExample(StreamExecutionEnvironment env) {
        DataStream<String> input = env.fromElements(
            "hello world",
            "flink streaming",
            "datastream api"
        );
        
        // 使用 flatMap 将句子拆分为单词
        DataStream<String> result = input.flatMap(
            (String sentence, Collector<String> out) -> {
                for (String word : sentence.split("\\s+")) {
                    out.collect(word);
                }
            }
        ).returns(Types.STRING);
        
        result.print("FlatMap Result");
    }

    /**
     * 示例 4：keyBy + sum 算子
     * 功能：统计每个单词出现的次数
     */
    private static void keyByExample(StreamExecutionEnvironment env) {
        DataStream<String> input = env.fromElements(
            "hello", "world", "hello", "flink", "world", "hello"
        );
        
        // 将单词转换为 (word, 1) 元组
        DataStream<Tuple2<String, Integer>> wordCounts = input
            .map(word -> Tuple2.of(word, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            // 按单词分组
            .keyBy(tuple -> tuple.f0)
            // 对计数字段求和
            .sum(1);
        
        wordCounts.print("KeyBy Result");
    }
}
