# Article 51 - Mini-Batch 聚合示例

本项目是「Apache Flink 从入门到专家」系列第 51 篇的配套代码，演示 Mini-Batch 聚合优化的配置与效果。

## 项目说明

- **MiniBatchExample.java**：Mini-Batch + Local-Global 两阶段聚合的完整示例，包含参数配置、模拟数据、聚合 SQL 和执行计划输出。

## 核心配置

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
```

## 环境要求

- JDK 11+
- Apache Flink 1.17+
- Maven 3.6+

## 运行方式

```bash
# 编译打包
mvn clean package

# 提交到 Flink 集群
flink run -c com.example.flink.MiniBatchExample target/article51-mini-batch-1.0-SNAPSHOT.jar
```

也可以在 IDE 中直接运行 `MiniBatchExample.main()`（需将 pom.xml 中 `provided` 改为 `compile`）。

## 预期输出

程序会打印 Mini-Batch 配置信息、执行计划（包含 `LocalGroupAggregate` 和 `GlobalGroupAggregate`）以及每个商品的聚合结果（订单数、总金额、平均金额）。
