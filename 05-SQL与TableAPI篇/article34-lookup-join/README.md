# Article 34 - 维表 Join 性能差？Lookup Join + 缓存策略优化指南

本项目是《Apache Flink 从入门到专家》系列第 34 篇的配套代码示例，演示 Flink SQL 中 Lookup Join 的使用方法和缓存优化策略。

## 项目结构

```
article34-lookup-join/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── LookupJoinExample.java          # 基础 Lookup Join 示例（PARTIAL 缓存）
    ├── AsyncLookupJoinExample.java     # 异步 Lookup Join 示例（Query Hints）
    └── CacheStrategyCompareExample.java # 缓存策略对比示例（PARTIAL vs FULL）
```

## 环境要求

- JDK 11+
- Maven 3.6+
- Apache Flink 1.17.2
- MySQL 5.7+ / 8.0+

## 数据库准备

运行示例前，需要在 MySQL 中创建测试数据库和维表：

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS flink_demo;
USE flink_demo;

-- 创建用户维表
CREATE TABLE user_dim (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL,
    user_level INT DEFAULT 1
);

-- 插入测试数据
INSERT INTO user_dim (user_id, user_name, user_level) VALUES
(1, '张三', 3), (2, '李四', 2), (3, '王五', 1),
(4, '赵六', 4), (5, '钱七', 2), (6, '孙八', 3),
(7, '周九', 1), (8, '吴十', 5), (9, '郑一', 2),
(10, '冯二', 3);

-- 创建商品维表
CREATE TABLE product_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

-- 插入测试数据
INSERT INTO product_dim (product_id, product_name, category, price) VALUES
(1, 'iPhone 15', '手机', 6999.00),
(2, 'MacBook Pro', '电脑', 14999.00),
(3, 'AirPods Pro', '耳机', 1899.00),
(4, 'iPad Air', '平板', 4799.00),
(5, 'Apple Watch', '手表', 2999.00);
```

## 运行方式

```bash
# 编译项目
mvn clean package

# 运行基础 Lookup Join 示例（带 PARTIAL 缓存）
mvn exec:java -Dexec.mainClass="com.example.flink.LookupJoinExample"

# 运行异步 Lookup Join 示例
mvn exec:java -Dexec.mainClass="com.example.flink.AsyncLookupJoinExample"

# 运行缓存策略对比示例
mvn exec:java -Dexec.mainClass="com.example.flink.CacheStrategyCompareExample"
```

## 示例说明

| 示例 | 核心功能 | 缓存模式 | 查询模式 |
|:---|:---|:---|:---|
| LookupJoinExample | 订单流关联用户维表 | PARTIAL | 同步 |
| AsyncLookupJoinExample | 高流量异步维表查询 | PARTIAL | 异步（Query Hints） |
| CacheStrategyCompareExample | PARTIAL vs FULL 缓存对比 | PARTIAL / FULL | 同步 |

## 关键知识点

1. **Lookup Join 语法**：`FOR SYSTEM_TIME AS OF proc_time` 是维表关联的标志性语法
2. **PARTIAL 缓存**：按需加载 + LRU 淘汰，适合大多数场景
3. **FULL 缓存**：启动时全量加载，适合小维表（< 10 万行）
4. **异步 Lookup**：通过 `LOOKUP` Hint 启用，提升高流量场景下的吞吐
5. **缓存过期策略**：`expire-after-write` 控制数据新鲜度，`expire-after-access` 淘汰冷数据

## 注意事项

- 运行前请修改 JDBC 连接参数（url、username、password）为你的实际配置
- 示例中的密码 `******` 需要替换为实际密码
- DataGen 连接器生成的 user_id 范围需要与维表中的数据匹配

## 相关文章

- [第 34 篇：维表 Join 性能差？](https://github.com/pingxin403/apache-flink-examples)
- [第 33 篇：流式 SQL 窗口](../article33-sql-windows/)
- [第 27 篇：异步 I/O 不阻塞](../../04-Connector与集成篇/article27-async-io/)
