# Article 22: 实时排行榜 TOP-N 示例

本示例演示如何使用 Flink 实现实时商品销量排行榜,展示窗口 TOP-N 的正确实现方式。

## 功能说明

- 模拟电商订单数据流
- 按商品ID分组,统计每分钟的销量
- 计算销量 TOP 10 的商品
- 实时输出排行榜

## 核心技术

### 两阶段聚合架构

**第一阶段:分组聚合**
- 按商品ID分组(keyBy)
- 使用滚动窗口(1分钟)
- 使用 AggregateFunction 增量聚合销量
- 输出每个商品在窗口内的总销量

**第二阶段:全局排序**
- 所有商品销量路由到同一个并行度
- 使用 KeyedProcessFunction + 定时器
- 在窗口结束后对所有商品排序
- 输出 TOP 10 商品

### 关键组件

1. **Order.java**: 订单数据模型
2. **ProductSales.java**: 商品销量统计模型
3. **SalesAggregateFunction.java**: 销量聚合函数(增量聚合)
4. **WindowResultFunction.java**: 窗口结果处理函数(附加窗口信息)
5. **TopNProcessFunction.java**: TOP-N 排序函数(使用定时器)
6. **OrderSource.java**: 模拟订单数据源
7. **TopNExample.java**: 主程序

## 运行环境

- Java 11+
- Apache Flink 1.17.2
- Maven 3.6+

## 编译运行

### 1. 编译项目

```bash
cd apache-flink-examples/03-时间与窗口篇/article22-topn
mvn clean package
```

### 2. 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.TopNExample"
```

或者直接在 IDE 中运行 `TopNExample.java`。

## 预期输出

程序会每分钟输出一次排行榜:

```
========================================
           TOP 10 商品销量排行榜
========================================
窗口结束时间: 2024-01-15 10:01:00.000
----------------------------------------
NO.1  | P001       | 销量: 1250  
NO.2  | P005       | 销量: 980   
NO.3  | P003       | 销量: 875   
NO.4  | P007       | 销量: 720   
NO.5  | P002       | 销量: 650   
NO.6  | P009       | 销量: 580   
NO.7  | P004       | 销量: 520   
NO.8  | P006       | 销量: 480   
NO.9  | P008       | 销量: 420   
NO.10 | P010       | 销量: 380   
========================================
```

## 代码说明

### 数据流处理流程

```
订单流(OrderSource)
  ↓
设置事件时间和 Watermark
  ↓
第一阶段:按商品ID分组聚合
  - keyBy(productId)
  - window(1分钟滚动窗口)
  - aggregate(销量累加)
  ↓
第二阶段:全局排序
  - keyBy("global")
  - process(TOP-N排序)
  ↓
输出排行榜
```

### 性能优化要点

1. **使用 AggregateFunction 而非 ProcessWindowFunction**
   - 增量聚合,内存占用小
   - 避免在内存中缓存所有订单

2. **两阶段聚合**
   - 第一阶段可以并行执行
   - 第二阶段只处理聚合后的结果,数据量小

3. **使用定时器确保数据完整性**
   - 在窗口结束后触发排序
   - 确保所有商品的销量都已到达

4. **及时清理状态**
   - 在定时器中清空状态
   - 避免内存泄漏

## 扩展练习

1. **修改窗口大小**: 将窗口从1分钟改为5分钟,观察排行榜的变化
2. **增加并行度**: 将第一阶段的并行度设置为10,观察性能变化
3. **使用滑动窗口**: 改用滑动窗口(窗口大小5分钟,滑动步长1分钟)
4. **输出到外部系统**: 将排行榜结果输出到 Redis 或 Kafka
5. **按类目统计**: 修改代码,实现按商品类目分别统计 TOP-N

## 常见问题

### Q1: 为什么第二阶段的并行度必须为1?

A: 因为需要对所有商品进行全局排序。如果并行度大于1,每个并行度只能看到部分商品,无法得到全局的 TOP-N。

### Q2: 如何处理数据倾斜?

A: 在第一阶段使用更细粒度的key(比如 `productId + random`),或者增加第一阶段的并行度。

### Q3: 如何处理延迟数据?

A: 使用 `allowedLateness()` 允许一定时间的延迟,并使用 `sideOutputLateData()` 将超时数据输出到侧输出流。

### Q4: 如何优化排序性能?

A: 使用堆排序(PriorityQueue)代替全排序,将复杂度从 O(N*logN) 降低到 O(N*logK)。

## 相关文章

- [第21篇: 会话窗口实战](../../21-会话窗口实战.md)
- [第23篇: 累积窗口](../../23-累积窗口.md)
- [第19篇: 窗口函数进化史](../../19-窗口函数进化史.md)

## 参考资料

- [Flink 官方文档 - Windows](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/)
- [Flink 官方文档 - Process Function](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/process_function/)
- [Flink 官方文档 - Event Time and Watermarks](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/time/)
