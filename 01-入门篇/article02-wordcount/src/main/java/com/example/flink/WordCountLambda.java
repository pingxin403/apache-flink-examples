package com.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式 WordCount 示例（Lambda 版本）
 * 
 * 功能：与 WordCount.java 相同，但使用 Lambda 表达式简化代码
 * 
 * 优点：
 * - 代码更简洁
 * - 减少样板代码
 * 
 * 注意事项：
 * - 使用 Lambda 时必须通过 .returns() 显式指定返回类型
 * - 否则 Flink 无法推断泛型信息，会导致运行时错误
 * 
 * @author 韩云朋
 * @version 1.0
 * @since 2024-01-15
 */
public class WordCountLambda {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 Socket 读取数据
        DataStream<String> text = env.socketTextStream("localhost", 9999);

        // 使用 Lambda 表达式进行数据转换
        DataStream<Tuple2<String, Integer>> wordCounts = text
                // Lambda 版本的 flatMap
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    // 按空格切分
                    for (String word : line.split("\\s+")) {
                        if (word.length() > 0) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                // 必须显式指定返回类型，否则 Flink 无法推断
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // 按单词分组
                .keyBy(value -> value.f0)
                // 对计数求和
                .sum(1);

        // 输出结果
        wordCounts.print();

        // 执行作业
        env.execute("Streaming WordCount (Lambda)");
    }
}
