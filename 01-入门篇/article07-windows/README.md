# Flink 窗口示例

本项目包含 Flink 三种基本窗口类型的代码示例，对应文章《窗口初体验：Tumbling、Sliding、Session 窗口代码实战》。

## 项目结构

```
article07-windows/
├── pom.xml                                    # Maven 配置文件
├── README.md                                  # 本文件
└── src/main/java/com/example/flink/
    ├── Order.java                             # 订单数据模型
    ├── OrderSource.java                       # 订单数据源
    ├── UserAction.java                        # 用户行为数据模型
    ├── UserActionSource.java                  # 用户行为数据源
    ├── TumblingWindowExample.java             # 滚动窗口示例
    ├── SlidingWindowExample.java              # 滑动窗口示例
    └── SessionWindowExample.java              # 会话窗口示例
```

## 环境要求

- Java 11 或更高版本
- Maven 3.6 或更高版本
- Apache Flink 1.17.2

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例

#### 运行 Tumbling Window 示例

```bash
# 方式一：使用 Maven 运行
mvn exec:java -Dexec.mainClass="com.example.flink.TumblingWindowExample"

# 方式二：使用 Java 命令运行
java -cp target/windows-example-1.0-SNAPSHOT.jar com.example.flink.TumblingWindowExample
```

**功能说明**：
- 统计每10秒的订单量
- 窗口大小固定，窗口之间不重叠
- 每个订单只属于一个窗口

**预期输出**：
```
Tumbling Window Result> (order_count, 15)
Tumbling Window Result> (order_count, 18)
Tumbling Window Result> (order_count, 12)
```

#### 运行 Sliding Window 示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.SlidingWindowExample"
```

**功能说明**：
- 统计最近30秒的订单量，每10秒更新一次
- 窗口大小30秒，滑动步长10秒
- 窗口之间有重叠，每个订单可能属于多个窗口

**预期输出**：
```
Sliding Window Result> (order_count, 45)
Sliding Window Result> (order_count, 52)
Sliding Window Result> (order_count, 48)
```

#### 运行 Session Window 示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.SessionWindowExample"
```

**功能说明**：
- 分析用户会话行为
- Gap设置为5秒：如果用户5秒内没有操作，就认为会话结束
- 窗口大小不固定，基于用户的实际活动情况

**预期输出**：
```
Session Window Result> (user_001, 5, 操作次数=5, 会话时长=12秒, 开始时间=10:30:15, 结束时间=10:30:27)
Session Window Result> (user_002, 3, 操作次数=3, 会话时长=8秒, 开始时间=10:30:20, 结束时间=10:30:28)
Session Window Result> (user_001, 7, 操作次数=7, 会话时长=18秒, 开始时间=10:30:35, 结束时间=10:30:53)
```

## 代码说明

### 1. Tumbling Window（滚动窗口）

```java
.window(TumblingEventTimeWindows.of(Time.seconds(10)))
```

**特点**：
- 窗口大小固定
- 窗口之间不重叠
- 每个数据只属于一个窗口

**适用场景**：
- 每分钟订单量统计
- 每小时PV/UV统计
- 固定周期报表

### 2. Sliding Window（滑动窗口）

```java
.window(SlidingEventTimeWindows.of(
    Time.seconds(30),  // 窗口大小
    Time.seconds(10)   // 滑动步长
))
```

**特点**：
- 窗口大小固定
- 窗口之间可以重叠
- 每个数据可能属于多个窗口

**适用场景**：
- 最近N分钟的订单趋势
- 移动平均计算
- 实时热搜榜

### 3. Session Window（会话窗口）

```java
.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
```

**特点**：
- 窗口大小不固定
- 基于活动间隔（Gap）划分窗口
- 如果两个事件之间的间隔超过Gap，就划分为两个窗口

**适用场景**：
- 用户会话分析
- 异常行为检测
- 物联网设备监控

## 关键概念

### Event Time vs Processing Time

本示例使用 **Event Time**（事件时间）：
- 基于数据自带的时间戳
- 结果准确、可重现
- 需要配置 Watermark 处理乱序数据

```java
.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
)
```

### Watermark

Watermark 用于处理乱序数据：
- `forBoundedOutOfOrderness(Duration.ofSeconds(5))`：允许5秒的乱序
- 当 Watermark 到达窗口结束时间时，窗口才会触发计算

## 常见问题

### Q1: 为什么窗口没有立即触发？

**A**: 窗口需要等待 Watermark 到达窗口结束时间才会触发。如果数据源停止发送数据，Watermark 不会推进，窗口就不会触发。

### Q2: 滑动窗口的步长可以大于窗口大小吗？

**A**: 不建议。如果步长大于窗口大小，会导致部分数据不被任何窗口覆盖，造成数据丢失。

### Q3: Session Window 的 Gap 如何设置？

**A**: 根据业务场景设置：
- 网站用户会话：30分钟
- 移动App会话：5-10分钟
- 物联网设备：根据设备特性调整

### Q4: 如何选择窗口类型？

**决策树**：
```
需要固定周期统计吗？
├─ 是 → 需要看趋势变化吗？
│   ├─ 是 → Sliding Window
│   └─ 否 → Tumbling Window
└─ 否 → 需要基于活动间隔吗？
    ├─ 是 → Session Window
    └─ 否 → 考虑自定义窗口
```

## 扩展阅读

- [Apache Flink 官方文档 - Windows](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/)
- [Flink 窗口机制详解](https://flink.apache.org/2020/07/30/demo-fraud-detection.html)
- [Event Time 和 Watermark](https://flink.apache.org/2021/03/05/watermarks.html)

## GitHub 仓库

完整代码请访问：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

## 相关文章

- 第05篇：EventTime、ProcessingTime、IngestionTime 深度对比
- 第06篇：Watermark 是什么？
- 第07篇：窗口初体验（本文）
- 第08篇：部署模式怎么选？

---

**作者**：韩云朋  
**系列**：Apache Flink 从入门到专家
