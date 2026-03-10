package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式 WordCount 示例
 * 
 * 功能：实时统计从 Socket 接收的文本中每个单词出现的次数
 * 
 * 运行步骤：
 * 1. 启动 Socket 服务器：nc -lk 9999
 * 2. 运行本程序
 * 3. 在 Socket 终端输入文本，观察实时统计结果
 * 
 * 示例输入：
 *   hello world
 *   hello flink
 *   flink is awesome
 * 
 * 示例输出：
 *   (hello,1)
 *   (world,1)
 *   (hello,2)
 *   (flink,1)
 *   (flink,2)
 *   (is,1)
 *   (awesome,1)
 * 
 * @author 韩云朋
 * @version 1.0
 * @since 2024-01-15
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        // StreamExecutionEnvironment 是 Flink 程序的入口，负责管理作业的生命周期
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为 1，方便本地调试时观察输出结果
        // 生产环境建议根据数据量和资源情况动态调整
        env.setParallelism(1);

        // 2. 从 Socket 读取数据（Source）
        // socketTextStream 会监听指定主机和端口，接收文本数据
        // 每接收到一行文本（以换行符分隔），就会产生一条数据流
        DataStream<String> text = env.socketTextStream("localhost", 9999);

        // 3. 数据转换（Transformation）
        DataStream<Tuple2<String, Integer>> wordCounts = text
                // 3.1 将每行文本按空格切分成单词
                // flatMap 是一对多的转换算子：输入一行文本，输出多个 (单词, 1) 元组
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                        // 按空格切分（支持多个连续空格）
                        String[] words = line.split("\\s+");
                        
                        // 输出 (单词, 1) 元组
                        for (String word : words) {
                            // 过滤空字符串
                            if (word.length() > 0) {
                                out.collect(new Tuple2<>(word, 1));
                            }
                        }
                    }
                })
                // 3.2 按单词分组
                // keyBy 会根据指定的 key（这里是单词）将数据路由到不同的并行实例
                // 相同单词的数据会被发送到同一个实例，保证聚合的正确性
                .keyBy(value -> value.f0)
                // 3.3 对每个单词的计数求和
                // sum(1) 表示对 Tuple2 的第 2 个字段（索引为 1）进行累加
                // Flink 会自动维护每个单词的累加状态
                .sum(1);

        // 4. 输出结果（Sink）
        // print() 会将结果输出到控制台
        // 生产环境通常输出到 Kafka、数据库、文件系统等
        wordCounts.print();

        // 5. 执行作业
        // Flink 采用懒执行模式：前面的代码只是定义了计算逻辑
        // 调用 execute() 才会真正提交作业并开始执行
        env.execute("Streaming WordCount");
    }
}
