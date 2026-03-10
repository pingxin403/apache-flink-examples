# DataStream 三要素实战拆解 - 代码示例

本项目包含《DataStream 三要素实战拆解：Source → Transformation → Sink》文章的配套代码示例。

## 项目结构

```
article04-datastream-operators/
├── pom.xml                                 # Maven 配置文件
├── README.md                               # 本文件
└── src/main/java/com/example/flink/
    ├── Order.java                          # 订单实体类
    ├── OrderMonitor.java                   # 订单实时监控示例（完整流程）
    ├── OperatorExamples.java               # 常用算子示例（map/filter/flatMap/keyBy）
    └── WindowExamples.java                 # 窗口算子示例（滚动窗口/滑动窗口）
```

## 环境要求

- **JDK**: 11 或更高版本
- **Maven**: 3.x
- **Flink**: 1.17.2

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例

#### 示例 1：订单实时监控（完整流程）

```bash
mvn exec:java -Dexec.mainClass=com.example.flink.OrderMonitor
```

**功能说明**：
- 从集合读取模拟订单数据
- 过滤出金额大于 1000 的订单
- 按用户 ID 分组
- 统计每个用户最近 1 分钟的订单总金额
- 输出结果到控制台

**预期输出**：
```
Filter: Order{userId='user1', amount=1500.0, timestamp=...} -> PASS
Filter: Order{userId='user2', amount=800.0, timestamp=...} -> REJECT
Filter: Order{userId='user1', amount=2000.0, timestamp=...} -> PASS
...
Result> (user1,3500.0)
Result> (user3,1200.0)
Result> (user2,1100.0)
```

#### 示例 2：常用算子演示

```bash
mvn exec:java -Dexec.mainClass=com.example.flink.OperatorExamples
```

**功能说明**：
- 演示 `map` 算子：字符串转大写
- 演示 `filter` 算子：过滤长度大于 5 的字符串
- 演示 `flatMap` 算子：句子拆分为单词
- 演示 `keyBy + sum` 算子：统计单词出现次数

#### 示例 3：窗口算子演示

```bash
mvn exec:java -Dexec.mainClass=com.example.flink.WindowExamples
```

**功能说明**：
- 演示滚动窗口（Tumbling Window）：每 10 秒统计一次
- 演示滑动窗口（Sliding Window）：每 5 秒统计最近 10 秒的数据

## 核心代码解析

### 1. Source（数据源）

```java
// 从集合读取数据（用于测试）
DataStream<Order> orderStream = env.fromElements(
    new Order("user1", 1500.0, System.currentTimeMillis()),
    new Order("user2", 800.0, System.currentTimeMillis())
);

// 生产环境通常使用 Kafka
// DataStream<Order> orderStream = env.addSource(new FlinkKafkaConsumer<>(...));
```

### 2. Transformation（数据转换）

```java
DataStream<Tuple2<String, Double>> result = orderStream
    // filter：过滤数据
    .filter(order -> order.getAmount() > 1000)
    // map：一对一转换
    .map(order -> Tuple2.of(order.getUserId(), order.getAmount()))
    // keyBy：按 key 分组
    .keyBy(tuple -> tuple.f0)
    // window：窗口操作
    .timeWindow(Time.minutes(1))
    // sum：聚合计算
    .sum(1);
```

### 3. Sink（数据输出）

```java
// 输出到控制台（用于测试）
result.print();

// 生产环境通常输出到 Kafka、数据库等
// result.addSink(new FlinkKafkaProducer<>(...));
// result.addSink(JdbcSink.sink(...));
```

## 常用算子说明

### map：一对一转换

```java
// 将字符串转换为大写
stream.map(String::toUpperCase)

// 提取订单金额
orderStream.map(order -> order.getAmount())
```

### filter：数据过滤

```java
// 过滤出金额大于 1000 的订单
orderStream.filter(order -> order.getAmount() > 1000)

// 过滤出非空字符串
stream.filter(s -> s != null && !s.isEmpty())
```

### flatMap：一对多转换

```java
// 将句子拆分为单词
sentences.flatMap((String sentence, Collector<String> out) -> {
    for (String word : sentence.split("\\s+")) {
        out.collect(word);
    }
}).returns(Types.STRING)
```

### keyBy：数据分组

```java
// 按用户 ID 分组
orderStream.keyBy(order -> order.getUserId())

// 按多个字段分组
orderStream.keyBy(order -> Tuple2.of(order.getUserId(), order.getRegion()))
```

### window：窗口操作

```java
// 滚动窗口：每 10 秒统计一次
keyedStream.timeWindow(Time.seconds(10))

// 滑动窗口：每 5 秒统计最近 10 秒的数据
keyedStream.timeWindow(Time.seconds(10), Time.seconds(5))
```

## 常见问题

### 1. Lambda 表达式类型推断问题

使用 Lambda 表达式时，需要显式指定返回类型：

```java
// 错误：Flink 无法推断返回类型
stream.map(s -> s.toUpperCase())

// 正确：显式指定返回类型
stream.map(s -> s.toUpperCase()).returns(Types.STRING)
```

### 2. 并行度设置

本地调试时建议设置并行度为 1，方便观察输出：

```java
env.setParallelism(1);
```

生产环境根据数据量和资源情况调整：

```java
env.setParallelism(4);  // 设置全局并行度
stream.setParallelism(2);  // 设置算子并行度
```

### 3. 时间语义

使用窗口操作时，需要提取时间戳和水位线：

```java
stream.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
)
```

## 扩展练习

1. **修改过滤条件**：将金额阈值改为 500，观察输出变化
2. **添加新算子**：在 `filter` 和 `map` 之间添加一个 `map` 算子，将金额转换为整数
3. **修改窗口大小**：将窗口大小改为 30 秒，观察聚合结果
4. **添加日志输出**：在每个算子中添加 `System.out.println`，观察数据流转过程

## 相关文章

- [第 01 篇：批处理已死？为什么现代系统都在用 Flink 做实时计算](../../Flink/01-入门篇/01-批处理已死.md)
- [第 02 篇：5 分钟跑通你的第一个 Flink 作业](../../Flink/01-入门篇/02-5分钟跑通你的第一个Flink作业.md)
- [第 03 篇：Flink 架构速览](../../Flink/01-入门篇/03-Flink架构速览.md)
- [第 04 篇：DataStream 三要素实战拆解](../../Flink/01-入门篇/04-DataStream三要素实战拆解.md)

## 参考资料

- [Apache Flink 官方文档 - DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/)
- [Flink 算子详解](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/)
- [窗口操作指南](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/)

## 许可证

本项目代码仅供学习使用。

---

**作者**：韩云朋  
**GitHub**：https://github.com/pingxin403/apache-flink-examples
