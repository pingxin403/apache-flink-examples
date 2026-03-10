# Article 47 - 数据倾斜怎么办？Salting + 局部聚合实战

本项目是「Apache Flink 从入门到专家」系列第 47 篇的配套代码示例，演示两种解决数据倾斜的核心方案。

## 示例说明

| 文件 | 说明 |
|:---|:---|
| `SaltingExample.java` | Key Salting（加盐打散）：给热点 Key 添加随机后缀，分散到多个 subtask，再去盐做最终聚合 |
| `TwoPhaseAggExample.java` | Local-Global 两阶段聚合：使用 AggregateFunction 做增量预聚合，配合 ProcessWindowFunction 输出最终结果 |

## 环境要求

- JDK 11+
- Maven 3.6+
- Apache Flink 1.17.2

## 编译与运行

```bash
# 编译
mvn clean compile

# 打包
mvn clean package

# 运行 Key Salting 示例
mvn exec:java -Dexec.mainClass="com.example.flink.SaltingExample"

# 运行两阶段聚合示例
mvn exec:java -Dexec.mainClass="com.example.flink.TwoPhaseAggExample"
```

## 关键参数

- **SALT_FACTOR**（SaltingExample）：盐值数量，建议设置为并行度的 2-4 倍。值越大打散效果越好，但二次聚合开销也越大。
- **窗口大小**：示例使用 10 秒的 Tumbling Processing Time 窗口，生产环境根据业务需求调整。

## 相关文章

- [第 47 篇：数据倾斜怎么办？Salting + 局部聚合实战](https://github.com/pingxin403/apache-flink-examples)
