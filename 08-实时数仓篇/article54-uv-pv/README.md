# UV/PV/留存率：实时指标计算的 3 种实现方式

> 配套文章：[《UV/PV/留存率：实时指标计算的 3 种实现方式》](../../../Flink/08-实时数仓篇/54-UV/PV/留存率.md)
>
> GitHub 仓库：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

## 项目说明

本项目演示如何使用 Flink 实时计算 PV、UV 等核心指标，包含三个示例：

| 示例类 | 说明 | 实现方式 |
|:---|:---|:---|
| `SqlPvUvExample` | Flink SQL 实现每分钟 PV 统计 | SQL Tumble 窗口 + COUNT(*) |
| `SqlPvUvCombinedExample` | Flink SQL 同时计算 PV + UV + 人均访问 | SQL COUNT(DISTINCT) 精确去重 |
| `DataStreamUvExample` | DataStream API 实现 PV/UV 统计 | ProcessWindowFunction + HashSet 精确去重 |

## UV 去重三种方案

| 方案 | 精度 | 内存占用 | 适用规模 | 本项目示例 |
|:---|:---|:---|:---|:---|
| SET 精确去重 | 100% | 高 | 日活 < 100 万 | `DataStreamUvExample`（HashSet） |
| HyperLogLog | ≈99% | 极低（~12KB） | 日活不限 | 文章中有代码片段 |
| RoaringBitmap | 100% | 中等 | 日活 < 1 亿（整数 ID） | 文章中有代码片段 |

## 环境要求

- JDK 11+
- Maven 3.6+
- Flink 1.17.1

## 编译与运行

```bash
# 编译
mvn clean package -DskipTests

# 运行 SQL PV 统计示例
mvn exec:java -Dexec.mainClass="com.example.flink.SqlPvUvExample"

# 运行 SQL PV+UV 联合统计示例
mvn exec:java -Dexec.mainClass="com.example.flink.SqlPvUvCombinedExample"

# 运行 DataStream UV 统计示例
mvn exec:java -Dexec.mainClass="com.example.flink.DataStreamUvExample"
```

## 核心思路

```
页面访问流 ──→ Tumble 窗口（1 分钟）
                ├── PV = COUNT(*)                    → 简单计数
                ├── UV = COUNT(DISTINCT user_id)      → 精确去重（SQL）
                └── UV = HashSet<user_id>.size()      → 精确去重（DataStream）
```

**选型建议**：
- 日活百万以下 → `COUNT(DISTINCT)`（SQL 原生支持，最简单）
- 日活千万以上 → HyperLogLog（内存极小，误差约 1%）
- 整数 ID + 需要精确 → RoaringBitmap（压缩位图）
