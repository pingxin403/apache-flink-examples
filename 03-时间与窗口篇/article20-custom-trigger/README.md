# Article 20: 自定义触发器

本示例演示如何使用 Flink 自定义触发器(Trigger)实现灵活的窗口触发逻辑。

## 项目结构

```
article20-custom-trigger/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── MetricEvent.java              # 监控指标事件
    ├── ThresholdTrigger.java         # 阈值触发器
    ├── CountOrTimeTrigger.java       # 计数或时间触发器
    ├── EarlyResultTrigger.java       # 周期性提前触发器
    ├── MonitorTrigger.java           # 监控触发器
    └── CustomTriggerExample.java     # 完整示例
```

## 核心概念

### 1. 触发器(Trigger)

触发器决定窗口何时触发计算,是否清理窗口状态。

**TriggerResult 四种返回值**:
- `CONTINUE`: 不触发,继续等待
- `FIRE`: 触发计算,但保留窗口状态
- `PURGE`: 清理窗口状态,但不触发计算
- `FIRE_AND_PURGE`: 触发计算并清理窗口

### 2. 触发器方法

```java
public abstract class Trigger<T, W extends Window> {
    // 每个元素到达时调用
    TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx);
    
    // 处理时间定时器触发时调用
    TriggerResult onProcessingTime(long time, W window, TriggerContext ctx);
    
    // 事件时间定时器触发时调用
    TriggerResult onEventTime(long time, W window, TriggerContext ctx);
    
    // 窗口合并时调用(仅用于会话窗口)
    void onMerge(W window, OnMergeContext ctx);
    
    // 窗口清理时调用
    void clear(W window, TriggerContext ctx);
}
```

## 示例说明

### 示例1: 阈值触发器

当指标值超过阈值时立即触发,否则等到窗口结束。

```java
public class ThresholdTrigger extends Trigger<MetricEvent, TimeWindow> {
    private final double threshold;
    
    @Override
    public TriggerResult onElement(MetricEvent element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        if (element.getValue() > threshold) {
            return TriggerResult.FIRE;  // 立即触发
        }
        return TriggerResult.CONTINUE;
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE;  // 窗口结束时触发并清理
    }
}
```

### 示例2: 计数或时间触发器

数据量达到阈值或窗口结束时触发。

```java
public class CountOrTimeTrigger<T> extends Trigger<T, TimeWindow> {
    private final long maxCount;
    
    @Override
    public TriggerResult onElement(T element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        count.add(1L);
        
        if (count.get() >= maxCount) {
            count.clear();
            return TriggerResult.FIRE;  // 达到数量阈值,触发
        }
        return TriggerResult.CONTINUE;
    }
}
```

### 示例3: 周期性提前触发器

在窗口结束前按固定间隔触发。

```java
public class EarlyResultTrigger<T> extends Trigger<T, TimeWindow> {
    private final long interval;
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;  // 窗口结束
        } else {
            // 提前触发,注册下一个定时器
            long nextFireTime = time + interval;
            if (nextFireTime < window.maxTimestamp()) {
                ctx.registerEventTimeTimer(nextFireTime);
            }
            return TriggerResult.FIRE;  // 提前触发但不清理
        }
    }
}
```

### 示例4: 监控触发器

正常每 5 分钟触发,但 QPS 或错误率超阈值时立即触发。

```java
public class MonitorTrigger extends Trigger<MetricEvent, TimeWindow> {
    private final long qpsThreshold = 10000;
    private final double errorRateThreshold = 0.05;
    
    @Override
    public TriggerResult onElement(MetricEvent element, long timestamp, 
                                   TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        
        // 更新状态
        ValueState<Long> qpsState = ctx.getPartitionedState(qpsStateDesc);
        long currentQps = qpsState.value() == null ? 0 : qpsState.value();
        qpsState.update(currentQps + element.getQps());
        
        // 检查是否需要立即触发
        if (currentQps + element.getQps() > qpsThreshold || 
            element.getErrorRate() > errorRateThreshold) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }
}
```

## 运行示例

### 1. 编译项目

```bash
cd apache-flink-examples/03-时间与窗口篇/article20-custom-trigger
mvn clean package
```

### 2. 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.CustomTriggerExample"
```

### 3. 预期输出

```
=== 示例1:阈值触发器 ===
阈值触发> MetricResult{count=3, totalQps=600, maxErrorRate=3.00%, avgValue=86.67}
阈值触发> MetricResult{count=5, totalQps=1500, maxErrorRate=5.00%, avgValue=72.00}

=== 示例2:计数或时间触发器 ===
计数触发> MetricResult{count=3, totalQps=600, maxErrorRate=3.00%, avgValue=86.67}
计数触发> MetricResult{count=2, totalQps=900, maxErrorRate=5.00%, avgValue=75.00}

=== 示例3:周期性提前触发器 ===
周期触发> MetricResult{count=2, totalQps=300, maxErrorRate=2.00%, avgValue=55.00}
周期触发> MetricResult{count=4, totalQps=1000, maxErrorRate=4.00%, avgValue=67.50}
周期触发> MetricResult{count=5, totalQps=1500, maxErrorRate=5.00%, avgValue=72.00}

=== 示例4:监控触发器 ===
监控触发> MetricResult{count=3, totalQps=600, maxErrorRate=8.00%, avgValue=65.00}
监控触发> MetricResult{count=5, totalQps=1500, maxErrorRate=5.00%, avgValue=72.00}
```

## 关键要点

### 1. 触发器状态管理

触发器可以使用状态来记录窗口内的信息:

```java
// 使用 ReducingState 记录计数
ReducingStateDescriptor<Long> countStateDesc = 
    new ReducingStateDescriptor<>("count", new Sum(), Long.class);
ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);

// 使用 ValueState 记录值
ValueStateDescriptor<Long> qpsStateDesc = 
    new ValueStateDescriptor<>("qps", Long.class);
ValueState<Long> qpsState = ctx.getPartitionedState(qpsStateDesc);
```

### 2. 定时器管理

触发器可以注册和删除定时器:

```java
// 注册事件时间定时器
ctx.registerEventTimeTimer(window.maxTimestamp());

// 注册处理时间定时器
ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000);

// 删除定时器
ctx.deleteEventTimeTimer(window.maxTimestamp());
```

### 3. 清理资源

在 `clear()` 方法中清理所有状态和定时器:

```java
@Override
public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
    ctx.getPartitionedState(countStateDesc).clear();
    ctx.getPartitionedState(qpsStateDesc).clear();
}
```

### 4. 性能优化

- 避免过于频繁的触发
- 及时清理状态,防止内存泄漏
- 避免重复注册定时器
- 合理选择 FIRE 和 FIRE_AND_PURGE

## 常见问题

### Q1: FIRE 和 FIRE_AND_PURGE 的区别?

- `FIRE`: 触发计算但保留窗口状态,窗口可以继续积累数据
- `FIRE_AND_PURGE`: 触发计算并清理窗口状态,窗口结束

### Q2: 触发器的状态是 Keyed State 还是 Operator State?

触发器的状态是 **Keyed State**,每个 key 的每个窗口都有独立的状态。

### Q3: 如何避免定时器泄漏?

使用状态记录定时器是否已注册,避免重复注册:

```java
ValueStateDescriptor<Boolean> timerRegistered = 
    new ValueStateDescriptor<>("timer", Boolean.class);

ValueState<Boolean> state = ctx.getPartitionedState(timerRegistered);
if (state.value() == null) {
    ctx.registerEventTimeTimer(window.maxTimestamp());
    state.update(true);
}
```

### Q4: 触发器可以用于全局窗口吗?

可以。全局窗口默认使用 `NeverTrigger`,需要自定义触发器才能触发计算。

## 相关文章

- [Article 19: 窗口函数进化史](../article19-window-functions/)
- [Article 18: 迟到数据别丢](../article18-late-data/)
- [Article 17: Watermark 生成策略](../article17-watermark-strategies/)

## 参考资料

- [Flink 官方文档 - Window Triggers](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/#triggers)
- [Flink 官方文档 - Evictors](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/#evictors)
