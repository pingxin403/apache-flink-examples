# Article 36 - 实时去重 & TOP-N 示例代码

本目录包含《实时去重 & TOP-N：ROW_NUMBER 和 DISTINCT 的正确用法》文章的配套代码示例。

## 项目结构

```
article36-dedup-topn/
├── pom.xml                                          # Maven 依赖配置
├── README.md                                        # 本文件
└── src/main/java/com/example/flink/
    ├── DeduplicationExample.java                    # ROW_NUMBER 去重示例
    └── TopNExample.java                             # 实时 TOP-N 排行榜示例
```

## 示例说明

### 1. DeduplicationExample - 实时去重

演示使用 `ROW_NUMBER()` 实现订单数据的实时去重：

- **按处理时间去重（保留第一条）**：`ORDER BY proc_time ASC`，适合日志去重、埋点去重
- **按事件时间去重（保留最新状态）**：`ORDER BY order_time DESC`，适合订单状态同步

核心 SQL 模式：
```sql
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id       -- 去重键
            ORDER BY proc_time ASC      -- 保留策略
        ) AS row_num
    FROM orders
) WHERE row_num = 1                     -- 只保留一条
```

### 2. TopNExample - 实时 TOP-N 排行榜

演示使用 `ROW_NUMBER()` 实现每个品类销量前 3 的商品排行榜：

- **先聚合再 TOP-N**：先按商品 GROUP BY 聚合销量，再对聚合结果做 TOP-N
- **开启 Mini-Batch 优化**：减少状态更新频率，提升高 QPS 场景下的吞吐量

核心 SQL 模式：
```sql
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY category           -- 分组键
            ORDER BY total_quantity DESC    -- 排序指标
        ) AS row_num
    FROM product_sales
) WHERE row_num <= 3                        -- 取前 N 名
```

## 运行环境

- JDK 11+
- Apache Flink 1.17.2
- Maven 3.6+

## 运行方式

```bash
# 编译项目
mvn clean compile

# 运行去重示例
mvn exec:java -Dexec.mainClass="com.example.flink.DeduplicationExample"

# 运行 TOP-N 示例
mvn exec:java -Dexec.mainClass="com.example.flink.TopNExample"
```

## 关键配置说明

| 配置项 | 推荐值 | 说明 |
|:---|:---|:---|
| `table.exec.state.ttl` | 根据业务周期 | 状态过期时间，按天去重设 24h |
| `table.exec.mini-batch.enabled` | true | 开启 Mini-Batch 优化 |
| `table.exec.mini-batch.allow-latency` | 5s | Mini-Batch 最大延迟 |
| `table.exec.mini-batch.size` | 1000 | Mini-Batch 最大缓存条数 |

## GitHub 仓库

完整代码请参考：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

## 相关文章

- [第 36 篇：实时去重 & TOP-N：ROW_NUMBER 和 DISTINCT 的正确用法](../../../Flink/05-SQL与TableAPI篇/36-实时去重与TOP-N.md)
- [Flink 官方文档：Deduplication](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/deduplication/)
- [Flink 官方文档：Top-N](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/topn/)
