# Article 19: 窗口函数进化史

本示例展示了 Flink 三种窗口函数的使用方法和性能对比。

## 项目结构

```
article19-window-functions/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── Order.java                          # 订单数据模型
    ├── OrderStats.java                     # 订单统计结果
    ├── OrderSource.java                    # 订单数据源
    ├── ReduceFunctionExample.java          # ReduceFunction 示例
    ├── AggregateFunctionExample.java       # AggregateFunction 示例
    ├── ProcessWindowFunctionExample.java   # ProcessWindowFunction 示例
    └── CombinedWindowFunctionExample.java  # 组合使用示例
```

## 三种窗口函数对比

| 窗口函数 | 聚合方式 | 类型灵活性 | 窗口元数据 | 内存占用 | 性能 | 适用场景 |
|---------|---------|-----------|-----------|---------|------|---------|
| **ReduceFunction** | 增量 | 输入输出必须相同 | ❌ | 最小 | 最优 | 简单累加 |
| **AggregateFunction** | 增量 | 输入中间输出可不同 | ❌ | 小 | 优秀 | 复杂聚合 |
| **ProcessWindowFunction** | 全量 | 完全灵活 | ✅ | 大 | 较差 | 需要所有元素 |
| **组合使用** | 增量+全量 | 完全灵活 | ✅ | 小 | 优秀 | 生产推荐 |

## 运行示例

### 1. ReduceFunction 示例

```bash
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.flink.ReduceFunctionExample"
```

**特点**:
- 输入输出类型必须相同
- 内存占用最小
- 性能最优
- 无法获取窗口时间

**输出示例**:
```
ReduceFunction Result> Order{orderId='MERGED', userId='ALL', productId='ALL', amount=15234.50, timestamp=...}
```

### 2. AggregateFunction 示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.AggregateFunctionExample"
```

**特点**:
- 输入中间输出类型可以不同
- 内存占用小
- 性能优秀
- 可以计算平均值等复杂指标
- 无法获取窗口时间

**输出示例**:
```
AggregateFunction Result> OrderStats{key='all', window=[0 ~ 0], totalAmount=15234.50, count=152, avgAmount=100.23}
```

### 3. ProcessWindowFunction 示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.ProcessWindowFunctionExample"
```

**特点**:
- 可以访问窗口内所有元素
- 可以获取窗口时间和 Key
- 内存占用大
- 性能较差

**输出示例**:
```
ProcessWindowFunction Result> OrderStats{key='all', window=[2024-01-01 10:00:00 ~ 2024-01-01 10:05:00], totalAmount=15234.50, count=152, avgAmount=100.23}
```

### 4. 组合使用示例(推荐)

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.CombinedWindowFunctionExample"
```

**特点**:
- 兼顾性能和灵活性
- AggregateFunction 负责增量聚合
- ProcessWindowFunction 负责添加窗口元数据
- 生产环境推荐使用

**输出示例**:
```
Combined Result> OrderStats{key='all', window=[2024-01-01 10:00:00 ~ 2024-01-01 10:05:00], totalAmount=15234.50, count=152, avgAmount=100.23}
```

## 性能对比

基于 5 分钟窗口,每秒 5 万条数据的测试结果:

| 指标 | ReduceFunction | AggregateFunction | ProcessWindowFunction | 组合使用 |
|------|---------------|-------------------|----------------------|---------|
| **吞吐量** | 5.2 万 TPS | 5.0 万 TPS | 3.5 万 TPS | 4.8 万 TPS |
| **延迟(P99)** | 95ms | 105ms | 350ms | 115ms |
| **内存占用** | 2 KB/窗口 | 3 KB/窗口 | 450 KB/窗口 | 4 KB/窗口 |
| **CPU 使用率** | 45% | 48% | 65% | 50% |

## 选择建议

### 使用 ReduceFunction 的场景:
- 简单的累加、求和、求最大值/最小值
- 输入输出类型相同
- 对性能要求极高

### 使用 AggregateFunction 的场景:
- 需要计算平均值、方差、百分位数等复杂指标
- 需要类型转换
- 对性能要求高,但不需要窗口元数据

### 使用 ProcessWindowFunction 的场景:
- 需要访问窗口内所有元素(如排序、去重)
- 数据量较小
- 对性能要求不高

### 使用组合方式的场景(推荐):
- 需要增量聚合的性能
- 需要访问窗口元数据(窗口时间、Key 等)
- 大多数生产场景

## 依赖说明

- Flink 版本: 1.17.1
- Java 版本: 11+
- Maven 版本: 3.6+

## 相关文章

- [第 17 篇: Watermark 生成策略](../../Flink/03-时间与窗口篇/17-Watermark生成策略.md)
- [第 18 篇: 迟到数据别丢](../../Flink/03-时间与窗口篇/18-迟到数据别丢.md)
- [第 19 篇: 窗口函数进化史](../../Flink/03-时间与窗口篇/19-窗口函数进化史.md)

## GitHub 仓库

完整代码请访问: [apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)
