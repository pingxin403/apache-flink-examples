# Article 32 - Table API 入门

本项目是《Table API 入门：环境配置、流表互转与执行模式》的配套代码示例。

## 项目结构

```
article32-table-api-intro/
├── pom.xml                          # Maven 依赖配置（含 Table API 依赖）
├── README.md
└── src/main/java/com/example/flink/
    ├── TableApiIntroExample.java         # Table API 基础用法（分组聚合）
    ├── StreamTableConversionExample.java # DataStream 与 Table 双向转换
    └── ExecutionModeExample.java         # 流模式 vs 批模式对比
```

## 环境要求

- JDK 11+
- Maven 3.6+
- Apache Flink 1.17.2

## 核心依赖

| 依赖 | 作用 |
|:---|:---|
| `flink-table-api-java-bridge` | Table API 与 DataStream 桥接层 |
| `flink-table-planner-loader` | 基于 Calcite 的查询优化器 |
| `flink-table-runtime` | Table API 运行时支持 |

## 运行方式

```bash
# 编译项目
mvn clean compile

# 运行 Table API 基础示例
mvn exec:java -Dexec.mainClass="com.example.flink.TableApiIntroExample"

# 运行流表互转示例
mvn exec:java -Dexec.mainClass="com.example.flink.StreamTableConversionExample"

# 运行执行模式对比示例
mvn exec:java -Dexec.mainClass="com.example.flink.ExecutionModeExample"
```

## 示例说明

### 1. TableApiIntroExample

演示 Table API 的核心用法：
- 创建 `StreamTableEnvironment`
- `fromDataStream` 将 DataStream 转为 Table
- 使用 Table API 和 SQL 两种方式做分组聚合
- `toChangelogStream` 将结果转回 DataStream

### 2. StreamTableConversionExample

演示 DataStream 与 Table 的双向转换：
- `fromDataStream` + `Schema.newBuilder()` 手动指定字段类型
- `toDataStream`：适用于 INSERT-only 的查询结果
- `toChangelogStream`：适用于包含 UPDATE/DELETE 的聚合结果

### 3. ExecutionModeExample

对比流模式和批模式的行为差异：
- 批模式：支持全局排序，结果是最终值
- 流模式：结果是 Changelog 形式（+I, -U, +U）

## 相关文章

- [第 32 篇：Table API 入门](https://github.com/pingxin403/apache-flink-examples)
- [Flink 官方文档：Table API & SQL](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/overview/)
