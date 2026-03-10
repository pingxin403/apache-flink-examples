# Article 17: Watermark 生成策略示例代码

本目录包含《Watermark 生成策略:周期性 vs 标点式,哪种更适合你?》一文的配套代码示例。

## 📋 示例列表

### 1. PeriodicWatermarkExample - 周期性 Watermark 示例

**文件**: `src/main/java/com/example/flink/PeriodicWatermarkExample.java`

**功能**: 演示如何使用周期性 Watermark 处理乱序数据

**核心要点**:
- 使用 `forBoundedOutOfOrderness()` 指定最大乱序时间(5 秒)
- Flink 每 1 秒自动生成一次 Watermark
- Watermark = 当前最大事件时间戳 - 最大乱序时间
- 模拟 80% 顺序数据 + 20% 延迟数据的真实场景

**运行方式**:
```bash
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.flink.PeriodicWatermarkExample"
```

**预期输出**:
```
生成事件: Event{userId='user_1', eventType='click', timestamp=1704096000000, time=2024-01-01 12:00:00}
生成事件: Event{userId='user_2', eventType='click', timestamp=1704095997000, time=2024-01-01 11:59:57}
窗口 [1704096000000 - 1704096010000] 用户 user_1 的事件数: 3
窗口 [1704096000000 - 1704096010000] 用户 user_2 的事件数: 2
```

---

### 2. PunctuatedWatermarkExample - 标点式 Watermark 示例

**文件**: `src/main/java/com/example/flink/PunctuatedWatermarkExample.java`

**功能**: 演示如何根据数据内容动态生成 Watermark

**核心要点**:
- 实现 `WatermarkGenerator` 接口
- 在 `onEvent()` 方法中判断是否生成 Watermark
- 适合批流混合场景:每批数据的最后一条记录生成 Watermark
- 模拟每 10 个事件为一批的场景

**运行方式**:
```bash
mvn exec:java -Dexec.mainClass="com.example.flink.PunctuatedWatermarkExample"
```

**预期输出**:
```
生成普通事件: Event{userId='user_1', eventType='click', timestamp=1704096000000, watermarkMarker=false}
生成普通事件: Event{userId='user_2', eventType='click', timestamp=1704096001000, watermarkMarker=false}
...
生成 Watermark 标记事件: Event{userId='user_3', eventType='click', timestamp=1704096009000, watermarkMarker=true}
生成标点式 Watermark: 1704096009000
窗口 [1704096000000 - 1704096010000] 用户 user_1 的事件数: 4
```

---

### 3. IdleSourceExample - 空闲源处理示例

**文件**: `src/main/java/com/example/flink/IdleSourceExample.java`

**功能**: 演示如何使用 `withIdleness()` 处理空闲数据源

**核心要点**:
- 模拟 Kafka 多分区场景:3 个分区,其中分区 2 空闲
- 使用 `withIdleness(Duration.ofSeconds(10))` 标记空闲源
- 空闲分区的 Watermark 不参与下游计算,避免拖累整体 Watermark
- 演示空闲源问题的解决方案

**运行方式**:
```bash
mvn exec:java -Dexec.mainClass="com.example.flink.IdleSourceExample"
```

**预期输出**:
```
分区 0 启动
分区 1 启动
分区 2 启动
分区 0 生成事件: Event{userId='user_1', eventType='click', timestamp=1704096000000}
分区 1 生成事件: Event{userId='user_2', eventType='click', timestamp=1704096000000}
分区 2 生成事件: Event{userId='user_3', eventType='click', timestamp=1704096000000}
...
分区 2 进入空闲状态...
窗口 [1704096000000 - 1704096010000] 用户 user_1 的事件数: 5
窗口 [1704096000000 - 1704096010000] 用户 user_2 的事件数: 4
```

---

## 🛠️ 环境要求

- **Java**: 11 或更高版本
- **Maven**: 3.6 或更高版本
- **Flink**: 1.17.2

## 📦 依赖说明

主要依赖:
- `flink-streaming-java`: Flink 流处理核心库
- `flink-clients`: Flink 客户端库
- `flink-connector-kafka`: Kafka 连接器(可选,用于实际 Kafka 集成)

## 🚀 快速开始

### 1. 编译项目

```bash
cd apache-flink-examples/03-时间与窗口篇/article17-watermark-strategies
mvn clean package
```

### 2. 运行示例

**方式一:使用 Maven**
```bash
# 运行周期性 Watermark 示例
mvn exec:java -Dexec.mainClass="com.example.flink.PeriodicWatermarkExample"

# 运行标点式 Watermark 示例
mvn exec:java -Dexec.mainClass="com.example.flink.PunctuatedWatermarkExample"

# 运行空闲源处理示例
mvn exec:java -Dexec.mainClass="com.example.flink.IdleSourceExample"
```

**方式二:使用 Flink 命令行**
```bash
# 打包
mvn clean package

# 提交到 Flink 集群
flink run -c com.example.flink.PeriodicWatermarkExample \
    target/article17-watermark-strategies-1.0-SNAPSHOT.jar
```

## 📊 核心概念对比

| 特性 | 周期性 Watermark | 标点式 Watermark | 空闲源处理 |
|------|-----------------|-----------------|-----------|
| **生成时机** | 固定时间间隔(默认 200ms) | 根据数据内容动态生成 | 超时无数据标记为空闲 |
| **实现复杂度** | 简单 | 中等 | 简单 |
| **性能开销** | 低 | 中等 | 低 |
| **适用场景** | 大多数场景 | 批流混合、有时间标记 | 多分区数据源 |

## 🔍 关键代码片段

### 周期性 Watermark 配置

```java
WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
```

### 标点式 Watermark 实现

```java
public class PunctuatedWatermarkGenerator implements WatermarkGenerator<Event> {
    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        if (event.isWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }
    }
}
```

### 空闲源配置

```java
WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofSeconds(10)) // 10 秒无数据标记为空闲
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
```

## 📚 相关文章

- [第 06 篇:Watermark 是什么?](../../01-入门篇/06-Watermark是什么.md)
- [第 17 篇:Watermark 生成策略](../../../Flink/03-时间与窗口篇/17-Watermark生成策略.md)
- [第 18 篇:迟到数据别丢!](../../../Flink/03-时间与窗口篇/18-迟到数据别丢.md)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request!

## 📄 许可证

本项目采用 Apache License 2.0 许可证。

---

**作者**: 韩云朋  
**GitHub**: https://github.com/pingxin403/apache-flink-examples
