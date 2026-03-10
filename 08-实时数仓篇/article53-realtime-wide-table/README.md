# 实时宽表构建：流式 Join + 维表关联最佳实践

> 配套文章：[《实时宽表构建：流式 Join + 维表关联最佳实践》](../../../Flink/08-实时数仓篇/53-实时宽表构建.md)
>
> GitHub 仓库：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

## 项目说明

本项目演示如何使用 Flink 构建实时宽表，包含三个示例：

| 示例类 | 说明 | Join 方式 |
|:---|:---|:---|
| `WideTableJoinExample` | **核心示例**：Interval Join + Lookup Join 组合构建完整宽表 | Interval Join + Lookup Join |
| `RegularJoinWideTableExample` | Regular Join 构建宽表（含 Retract 输出） | Regular Join |
| `DataStreamWideTableExample` | DataStream API 实现 Interval Join 宽表 | DataStream Interval Join |

## 环境要求

- JDK 11+
- Maven 3.6+
- Flink 1.17.1
- MySQL 5.7+（Lookup Join 示例需要）

## MySQL 建表 SQL

`WideTableJoinExample` 中的 Lookup Join 需要以下 MySQL 表：

```sql
CREATE DATABASE IF NOT EXISTS flink_demo;
USE flink_demo;

-- 用户维表
CREATE TABLE user_dim (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(50),
    city VARCHAR(50),
    vip_level INT
);

INSERT INTO user_dim VALUES
(1, '张三', '北京', 3),
(2, '李四', '上海', 2),
(3, '王五', '广州', 1),
(4, '赵六', '深圳', 3),
(5, '钱七', '杭州', 2);

-- 商品维表
CREATE TABLE product_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50)
);

INSERT INTO product_dim VALUES
(1, '无线耳机', '数码', 'BrandA'),
(2, '运动鞋', '服饰', 'BrandB'),
(3, '智能手机', '数码', 'BrandC'),
(4, '笔记本电脑', '数码', 'BrandD'),
(5, '连衣裙', '服饰', 'BrandE');
```

## 编译与运行

```bash
# 编译
mvn clean package -DskipTests

# 运行 DataStream 示例（无需 MySQL）
mvn exec:java -Dexec.mainClass="com.example.flink.DataStreamWideTableExample"

# 运行 Regular Join 示例（无需 MySQL，使用 DataGen）
mvn exec:java -Dexec.mainClass="com.example.flink.RegularJoinWideTableExample"

# 运行完整宽表示例（需要 MySQL）
mvn exec:java -Dexec.mainClass="com.example.flink.WideTableJoinExample"
```

## 核心思路

```
订单流 ──┐
         ├── Interval Join（0~30min）──┐
支付流 ──┘                             ├── Lookup Join ── 用户维表
                                       ├── Lookup Join ── 商品维表
                                       └── DWD 订单宽表
```

**选型原则**：
- 流流 Join 拼事实 → 有时间因果用 Interval Join，无时间约束用 Regular Join + TTL
- 维表 Join 补维度 → Lookup Join + 缓存策略（PARTIAL 推荐）
