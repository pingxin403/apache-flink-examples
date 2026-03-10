# Article 35 - 双流 Join 怎么做？Window Join vs Interval Join 对比

本项目是《Apache Flink 从入门到专家》系列第 35 篇的配套代码示例，演示 Flink 中两条实时流之间的 Join 操作，包括 Window Join 和 Interval Join 两种方案。

## 项目结构

```
article35-stream-join/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── WindowJoinExample.java              # Tumble Window Join 示例（SQL）
    ├── IntervalJoinExample.java            # Interval Join 示例（SQL）
    └── DataStreamIntervalJoinExample.java  # Interval Join 示例（DataStream API）
```

## 环境要求

- JDK 11+
- Maven 3.6+
- Apache Flink 1.17.2

## 运行方式

```bash
# 编译项目
mvn clean package

# 运行 Window Join 示例（Tumble 窗口，5 分钟窗口内匹配）
mvn exec:java -Dexec.mainClass="com.example.flink.WindowJoinExample"

# 运行 Interval Join 示例（SQL，订单后 0~15 分钟内匹配）
mvn exec:java -Dexec.mainClass="com.example.flink.IntervalJoinExample"

# 运行 DataStream API Interval Join 示例（带模拟数据，可观察匹配结果）
mvn exec:java -Dexec.mainClass="com.example.flink.DataStreamIntervalJoinExample"
```

## 示例说明

| 示例 | API | Join 类型 | 时间约束 | 输出时机 |
|:---|:---|:---|:---|:---|
| WindowJoinExample | SQL | Window Join | 5 分钟 Tumble 窗口 | 窗口关闭时 |
| IntervalJoinExample | SQL | Interval Join | 订单后 0~15 分钟 | 匹配成功立即输出 |
| DataStreamIntervalJoinExample | DataStream | Interval Join | 订单后 0~15 分钟 | 匹配成功立即输出 |

## 关键知识点

1. **Window Join**：两条流按相同窗口划分，同一窗口内的数据才能匹配，窗口关闭后批量输出
2. **Interval Join**：为每条数据定义时间范围，对面的数据落在范围内即可匹配，匹配后立即输出
3. **状态 TTL**：双流 Join 的状态大小取决于时间范围内的数据量，务必设置合理的 TTL
4. **水位线对齐**：双流 Join 依赖水位线推进来清理过期状态，注意空闲源检测
5. **选型建议**：数据按时间段聚合用 Window Join，有因果时序关系用 Interval Join

## 注意事项

- WindowJoinExample 和 IntervalJoinExample 使用 DataGen 连接器生成随机数据，order_id 为随机字符串，匹配概率较低，主要用于演示语法
- DataStreamIntervalJoinExample 使用预定义数据，可以清晰观察匹配和不匹配的情况
- 生产环境中务必配置 `table.exec.state.ttl` 和 `table.exec.source.idle-timeout`

## 相关文章

- [第 35 篇：双流 Join 怎么做？](https://github.com/pingxin403/apache-flink-examples)
- [第 34 篇：维表 Join 性能差？](../article34-lookup-join/)
- [第 33 篇：流式 SQL 窗口](../article33-sql-windows/)
