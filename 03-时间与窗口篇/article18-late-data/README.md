# Article 18: 迟到数据处理示例

本示例演示如何使用 Flink 的 Allowed Lateness 和 Side Output 机制处理迟到数据。

## 项目结构

```
article18-late-data/
├── pom.xml                                    # Maven 配置文件
├── README.md                                  # 项目说明文档
└── src/main/java/com/example/flink/
    ├── Order.java                             # 订单实体类
    ├── OrderStats.java                        # 订单统计结果类
    ├── LateDataHandlingExample.java           # 主程序：迟到数据处理示例
    └── OrderSourceWithLateData.java           # 数据源：模拟包含迟到数据的订单流
```

## 核心概念

### 1. 迟到数据的定义

迟到数据是指事件时间戳小于当前 Watermark 的数据：

```
迟到数据 = 事件时间戳 < 当前 Watermark
```

### 2. Allowed Lateness（允许延迟）

Allowed Lateness 机制允许窗口在触发后继续保持打开状态，等待迟到数据：

- **窗口首次触发**：Watermark 超过窗口结束时间
- **窗口保持打开**：继续等待迟到数据（Allowed Lateness 时间内）
- **窗口重新计算**：每次有迟到数据到达，窗口重新计算并输出结果
- **窗口最终关闭**：Watermark 超过 `窗口结束时间 + Allowed Lateness`

### 3. Side Output（侧输出流）

Side Output 机制用于捕获超过 Allowed Lateness 的迟到数据：

- **定义侧输出标签**：`OutputTag<Order> lateDataTag`
- **配置侧输出**：`.sideOutputLateData(lateDataTag)`
- **获取侧输出流**：`.getSideOutput(lateDataTag)`
- **处理迟到数据**：对侧输出流进行单独处理

## 示例说明

### 数据生成策略

`OrderSourceWithLateData` 模拟了三种数据：

1. **正常数据**（90%）：事件时间 = 当前时间
2. **轻微延迟数据**（8%）：延迟 5-15 秒（在 Watermark 容忍范围内）
3. **严重延迟数据**（2%）：延迟 20-50 秒（超过 Watermark + Allowed Lateness）

### 配置参数

- **Watermark 延迟**：10 秒
- **Allowed Lateness**：30 秒
- **窗口类型**：5 分钟滚动窗口

### 时间线分析

```
窗口：[10:00:00, 10:05:00)

10:05:10 → Watermark 到达，窗口首次触发，输出结果
10:05:40 → Watermark 到达，窗口最终关闭
超过 10:05:40 的迟到数据 → 进入侧输出流
```

## 运行示例

### 1. 编译项目

```bash
mvn clean compile
```

### 2. 运行主程序

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.LateDataHandlingExample"
```

### 3. 观察输出

**主流输出**（正常结果）：

```
[主流] 窗口 [2024-01-15 10:00:00, 2024-01-15 10:05:00): 订单总额 = 125000.00 元, 订单数量 = 250, 版本 = 1
[主流] 窗口 [2024-01-15 10:00:00, 2024-01-15 10:05:00): 订单总额 = 126500.00 元, 订单数量 = 253, 版本 = 2
[主流] 窗口 [2024-01-15 10:00:00, 2024-01-15 10:05:00): 订单总额 = 127200.00 元, 订单数量 = 255, 版本 = 3
```

**侧输出流输出**（迟到数据告警）：

```
[侧输出] 迟到订单: orderId=ORDER_00123, eventTime=2024-01-15 10:04:55, latency=55000ms, amount=350.00
[侧输出] 迟到订单: orderId=ORDER_00456, eventTime=2024-01-15 10:04:58, latency=72000ms, amount=520.00
```

## 关键代码解析

### 1. 配置 Watermark

```java
WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
```

- `forBoundedOutOfOrderness(Duration.ofSeconds(10))`：允许 10 秒乱序
- `withTimestampAssigner`：从订单对象中提取事件时间戳

### 2. 配置窗口和迟到数据处理

```java
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.allowedLateness(Time.seconds(30))  // 允许 30 秒延迟
.sideOutputLateData(LATE_DATA_TAG)  // 捕获超时迟到数据
.aggregate(new OrderAggregateFunction())
```

- `allowedLateness(Time.seconds(30))`：窗口在触发后继续保持 30 秒
- `sideOutputLateData(LATE_DATA_TAG)`：将超时迟到数据输出到侧输出流

### 3. 处理侧输出流

```java
DataStream<Order> lateData = result.getSideOutput(LATE_DATA_TAG);
lateData.map(order -> {
    // 处理迟到数据：打印告警、写入日志、补偿计算等
    return "Late order: " + order.getOrderId();
}).print();
```

## 最佳实践

### 1. 参数配置建议

| 场景 | Watermark 延迟 | Allowed Lateness | 说明 |
|------|---------------|------------------|------|
| 实时大屏 | 5-10 秒 | 30-60 秒 | 对延迟敏感，但需要保证准确性 |
| 实时风控 | 1-3 秒 | 10-30 秒 | 对延迟极度敏感，迟到数据单独处理 |
| 实时数仓 | 30-60 秒 | 5-10 分钟 | 对准确性要求高，可以容忍一定延迟 |

### 2. 下游系统适配

窗口会多次输出结果，下游系统需要支持更新语义：

- **MySQL**：使用 `INSERT ... ON DUPLICATE KEY UPDATE`
- **Redis**：使用 `SET` 覆盖更新
- **Elasticsearch**：使用 `_id` 覆盖更新

### 3. 监控迟到数据

监控侧输出流的数据量，如果超过 1%，说明配置需要调整：

```java
lateData
    .map(order -> 1)
    .timeWindowAll(Time.minutes(1))
    .sum(0)
    .map(count -> {
        if (count > threshold) {
            // 触发告警
        }
        return count;
    });
```

## 扩展阅读

- [Flink 官方文档 - Allowed Lateness](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/#allowed-lateness)
- [Flink 官方文档 - Side Outputs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/)
- [流式系统中的时间处理](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)

## 相关文章

- 第 06 篇：Watermark 是什么？
- 第 17 篇：Watermark 生成策略
- 第 19 篇：窗口函数进化史
