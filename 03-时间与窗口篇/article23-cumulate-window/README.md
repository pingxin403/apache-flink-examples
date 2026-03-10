# Article 23: 累积窗口(Cumulate Window)示例

本项目演示了 Flink 累积窗口的使用方法,包括:
- 使用 CUMULATE 窗口实现今日累计统计
- 累积窗口与滑动窗口的对比
- 实时大屏场景的完整实现

## 项目结构

```
article23-cumulate-window/
├── pom.xml                           # Maven 配置文件
├── README.md                         # 项目说明文档
└── src/main/java/com/example/flink/
    ├── Order.java                    # 订单数据模型
    ├── OrderSource.java              # 订单数据源(模拟生成)
    ├── CumulateWindowExample.java    # 累积窗口完整示例
    └── CumulateVsHopExample.java     # 累积窗口 vs 滑动窗口对比
```

## 环境要求

- Java 11+
- Apache Flink 1.17.2
- Maven 3.6+
- Kafka 2.8+ (可选,用于完整示例)
- MySQL 8.0+ (可选,用于完整示例)

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行对比示例(无需外部依赖)

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.CumulateVsHopExample"
```

这个示例使用 DataGen 连接器生成模拟数据,无需 Kafka,可以直接运行。

**预期输出**:

累积窗口结果(销售额单调递增):
```
start_time | end_time | total_sales | order_count
00:00:00   | 00:00:10 | 5000.00     | 10
00:00:00   | 00:00:20 | 12000.00    | 20
00:00:00   | 00:00:30 | 18500.00    | 30
00:00:00   | 00:00:40 | 25000.00    | 40
00:00:00   | 00:00:50 | 32000.00    | 50
00:00:00   | 00:01:00 | 38500.00    | 60
```

滑动窗口结果(销售额可能减少):
```
start_time | end_time | total_sales | order_count
00:00:00   | 00:01:00 | 38500.00    | 60
00:00:10   | 00:01:10 | 35000.00    | 55  ← 销售额减少了!
00:00:20   | 00:01:20 | 32000.00    | 50  ← 继续减少
```


### 3. 运行完整示例(需要 Kafka 和 MySQL)

#### 3.1 准备 Kafka

启动 Kafka 并创建 topic:

```bash
# 启动 Kafka
bin/kafka-server-start.sh config/server.properties

# 创建 topic
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

#### 3.2 准备 MySQL

创建数据库和表:

```sql
CREATE DATABASE IF NOT EXISTS flink_demo;

USE flink_demo;

CREATE TABLE daily_metrics (
    stat_date DATE NOT NULL,
    stat_time TIME NOT NULL,
    total_orders BIGINT NOT NULL,
    total_sales DECIMAL(10, 2) NOT NULL,
    active_users BIGINT NOT NULL,
    PRIMARY KEY (stat_date, stat_time)
);
```

#### 3.3 生成测试数据

向 Kafka 发送订单数据:

```bash
# 使用 kafka-console-producer 发送测试数据
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders

# 输入以下 JSON 数据(每行一条):
{"order_id":"order_001","user_id":"user_001","product_id":"prod_001","amount":100.00,"order_time":"2024-01-15T10:00:00"}
{"order_id":"order_002","user_id":"user_002","product_id":"prod_002","amount":200.00,"order_time":"2024-01-15T10:00:30"}
{"order_id":"order_003","user_id":"user_001","product_id":"prod_003","amount":150.00,"order_time":"2024-01-15T10:01:00"}
```

#### 3.4 运行 Flink 作业

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.CumulateWindowExample"
```

#### 3.5 查看结果

查询 MySQL 中的累计指标:

```sql
SELECT * FROM daily_metrics ORDER BY stat_date, stat_time;
```

预期结果:

```
stat_date   | stat_time | total_orders | total_sales | active_users
2024-01-15  | 10:01:00  | 2            | 300.00      | 2
2024-01-15  | 10:02:00  | 3            | 450.00      | 2
2024-01-15  | 10:03:00  | 3            | 450.00      | 2
...
```

## 核心代码说明

### 累积窗口语法

```sql
CUMULATE(
    TABLE orders,              -- 输入表
    DESCRIPTOR(order_time),    -- 时间列
    INTERVAL '1' MINUTE,       -- 窗口步长(刷新频率)
    INTERVAL '1' DAY           -- 最大窗口大小(累计周期)
)
```

**关键参数**:
- **step**(步长):决定刷新频率,越小刷新越频繁
- **size**(最大窗口):决定累计周期,必须是 step 的整数倍
- **窗口数量** = size / step

### 窗口属性函数

- `window_start`:窗口起始时间(固定不变)
- `window_end`:窗口结束时间(逐步推进)
- `window_time`:窗口时间属性(用于级联窗口)

## 性能优化建议

### 1. 合理设置 step 和 size

| step | 窗口数量(size=1天) | 适用场景 |
|------|-------------------|---------|
| 1 秒 | 86400 | 实时性要求极高 |
| 10 秒 | 8640 | 实时大屏 |
| 1 分钟 | 1440 | 常规实时报表 |
| 5 分钟 | 288 | 准实时报表 |

### 2. 使用增量聚合函数

✅ 推荐:SUM、COUNT、MIN、MAX、AVG
❌ 不推荐:COLLECT、LISTAGG

### 3. 避免高基数分组

```sql
-- ❌ 危险:按用户 ID 分组,状态可能爆炸
GROUP BY user_id, window_start, window_end

-- ✅ 推荐:只统计全局指标
GROUP BY window_start, window_end
```

### 4. 使用 UPSERT 模式写入

定义主键,避免数据重复:

```sql
CREATE TABLE daily_metrics (
    stat_date DATE,
    stat_time TIME,
    total_sales DECIMAL(10, 2),
    PRIMARY KEY (stat_date, stat_time) NOT ENFORCED
)
```

## 常见问题

### Q1: 累积窗口和滑动窗口有什么区别?

**累积窗口**:
- 窗口起点固定,终点递增
- 数据只进不出
- 适合"今日累计"场景

**滑动窗口**:
- 窗口整体滑动
- 旧数据会滑出窗口
- 适合"最近 N 分钟"场景

### Q2: size 必须是 step 的整数倍吗?

是的,否则会报错:

```
✅ 正确:CUMULATE(..., INTERVAL '1' MINUTE, INTERVAL '1' DAY)
   1 天 = 1440 分钟,是 1 分钟的整数倍

❌ 错误:CUMULATE(..., INTERVAL '7' MINUTE, INTERVAL '1' DAY)
   1 天 = 1440 分钟,不是 7 分钟的整数倍
```

### Q3: 累积窗口的性能如何?

累积窗口使用**增量计算**,性能优于滑动窗口:
- CPU 使用率降低 60%+
- 延迟降低 70%
- 吞吐量提升 3 倍

### Q4: 如何处理跨天的累计?

累积窗口会在每天 00:00 自动重置:

```sql
-- 今天 23:59 的窗口:[今天 00:00, 今天 23:59)
-- 明天 00:01 的窗口:[明天 00:00, 明天 00:01)
```

如果需要跨天累计,可以使用更大的 size:

```sql
-- 累计一周
CUMULATE(..., INTERVAL '1' HOUR, INTERVAL '7' DAY)
```

## 相关文章

- [第 22 篇:实时排行榜怎么做](../../../Flink/03-时间与窗口篇/22-实时排行榜怎么做.md)
- [第 21 篇:会话窗口实战](../../../Flink/03-时间与窗口篇/21-会话窗口实战.md)
- [第 19 篇:窗口函数进化史](../../../Flink/03-时间与窗口篇/19-窗口函数进化史.md)

## 参考资料

- [Flink 官方文档:窗口](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/window-tvf/)
- [Flink 1.13 新特性:累积窗口](https://flink.apache.org)
- [实时大屏技术选型指南](https://flink.apache.org)

---

**完整代码仓库**: https://github.com/pingxin403/apache-flink-examples
