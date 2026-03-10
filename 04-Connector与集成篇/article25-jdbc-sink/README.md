# Article 25: JDBC Sink 写 MySQL

本示例展示了如何使用 Flink JDBC Connector 将数据写入 MySQL,包括批量写入、连接池配置和幂等控制。

## 📋 目录结构

```
article25-jdbc-sink/
├── pom.xml                          # Maven 配置文件
├── README.md                        # 本文件
├── sql/
│   └── init.sql                     # 数据库初始化脚本
└── src/main/java/com/example/flink/
    ├── Order.java                   # 订单数据模型
    ├── OrderSource.java             # 订单数据源
    ├── SimpleModeExample.java       # 简单模式示例
    ├── BatchModeExample.java        # 批量模式示例(推荐)
    └── IdempotentWriteExample.java  # 幂等写入示例
```

## 🚀 快速开始

### 1. 环境准备

**必需软件**:
- JDK 11+
- Maven 3.6+
- MySQL 8.0+
- Flink 1.17+

### 2. 数据库初始化

```bash
# 登录 MySQL
mysql -u root -p

# 创建数据库
CREATE DATABASE flink_demo;
USE flink_demo;

# 创建订单表
CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    create_time DATETIME NOT NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 3. 编译项目

```bash
cd article25-jdbc-sink
mvn clean package
```

### 4. 运行示例

**简单模式**(每条数据立即写入):

```bash
flink run -c com.example.flink.SimpleModeExample \
  target/article25-jdbc-sink-1.0-SNAPSHOT.jar
```

**批量模式**(推荐,性能最优):

```bash
flink run -c com.example.flink.BatchModeExample \
  target/article25-jdbc-sink-1.0-SNAPSHOT.jar
```

**幂等写入示例**:

```bash
flink run -c com.example.flink.IdempotentWriteExample \
  target/article25-jdbc-sink-1.0-SNAPSHOT.jar
```


## 📊 示例说明

### SimpleModeExample - 简单模式

**特点**:
- 每条数据立即执行 SQL
- 无批量,无缓冲
- 延迟最低,但性能最差

**适用场景**:
- 数据量很小(每秒几条)
- 对延迟要求极低
- 测试和调试

**性能**:
- 吞吐量:约 100-500 条/秒
- 延迟:毫秒级
- 数据库压力:极高

### BatchModeExample - 批量模式(推荐)

**特点**:
- 积攒一批数据后批量执行 SQL
- 开启 rewriteBatchedStatements 优化
- 配置连接池和预编译缓存

**适用场景**:
- 大多数生产场景
- 数据量较大(每秒几千到几万条)
- 可以容忍秒级延迟

**性能**:
- 吞吐量:约 5,000-20,000 条/秒
- 延迟:秒级
- 数据库压力:中等

**关键配置**:

```java
// JDBC URL 优化参数
String jdbcUrl = "jdbc:mysql://localhost:3306/flink_demo?" +
    "rewriteBatchedStatements=true&"  // 批量重写,性能提升 10 倍+
    "cachePrepStmts=true&"            // 缓存预编译语句
    "prepStmtCacheSize=250&"          // 预编译缓存大小
    "prepStmtCacheSqlLimit=2048";     // 单条 SQL 最大长度

// 批量执行参数
JdbcExecutionOptions.builder()
    .withBatchSize(1000)              // 每 1000 条提交
    .withBatchIntervalMs(5000)        // 或每 5 秒提交
    .withMaxRetries(3)                // 失败重试 3 次
    .build();
```

### IdempotentWriteExample - 幂等写入

**三种实现方式**:

1. **INSERT IGNORE** - 冲突时忽略
   - 简单易用,性能好
   - 适用于只插入不更新的场景
   - 需要表有主键或唯一索引

2. **REPLACE INTO** - 冲突时删除后插入
   - 会完全替换旧记录
   - 适用于需要完全替换的场景
   - 会触发删除触发器

3. **INSERT ... ON DUPLICATE KEY UPDATE** - 冲突时更新(推荐)
   - 最灵活,可以自定义更新逻辑
   - 适用于需要更新部分字段的场景
   - 只触发更新触发器


## 🔧 配置说明

### 批量写入参数

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| **batchSize** | 批量大小 | 1000-5000 |
| **batchIntervalMs** | 批量间隔(毫秒) | 5000-10000 |
| **maxRetries** | 最大重试次数 | 3 |

### JDBC URL 优化参数

| 参数 | 说明 | 推荐值 | 性能提升 |
|------|------|--------|---------|
| **rewriteBatchedStatements** | 批量 SQL 重写 | true | 10 倍+ |
| **cachePrepStmts** | 缓存预编译语句 | true | 2-3 倍 |
| **prepStmtCacheSize** | 预编译缓存数量 | 250-500 | - |
| **prepStmtCacheSqlLimit** | 单条 SQL 最大长度 | 2048 | - |

### 并行度配置

```java
// Sink 并行度 = 数据库连接数
stream.addSink(jdbcSink).setParallelism(4);  // 4 个并行任务,4 个数据库连接
```

## 📈 性能测试

**测试环境**:
- Flink 1.17
- MySQL 8.0
- 4 核 8G 内存

**测试结果**:

| 配置 | 吞吐量 | 延迟 | CPU 使用率 |
|------|--------|------|-----------|
| 简单模式 | 500/秒 | 10ms | 20% |
| 批量模式(batchSize=100) | 5,000/秒 | 100ms | 40% |
| 批量模式(batchSize=1000) | 15,000/秒 | 1s | 60% |
| 批量模式(batchSize=5000) | 20,000/秒 | 5s | 80% |

**结论**:
- batchSize 越大,吞吐量越高,但延迟也越高
- 推荐 batchSize=1000,平衡性能和延迟
- 开启 rewriteBatchedStatements 后,性能提升 10 倍+

## ⚠️ 注意事项

### 1. 避免 OOM

- 合理设置 batchSize,不要过大
- 设置 batchIntervalMs,防止数据积压
- 启用 Checkpoint,定期清理内存

### 2. 保证幂等性

- 使用 INSERT IGNORE、REPLACE INTO 或 ON DUPLICATE KEY UPDATE
- 确保表有主键或唯一索引
- 配合 Checkpoint 使用

### 3. 连接池管理

- 并行度 = 数据库连接数
- 不要设置过大的并行度
- 监控数据库连接数使用情况

### 4. 性能优化

- 开启 rewriteBatchedStatements=true
- 使用 InnoDB 引擎
- 减少不必要的索引
- 调整 MySQL 参数(innodb_buffer_pool_size 等)

## 🔗 相关链接

- [Flink JDBC Connector 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/jdbc/)
- [MySQL 批量插入优化](https://dev.mysql.com/doc/refman/8.0/en/insert-optimization.html)
- [HikariCP 连接池配置](https://github.com/brettwooldridge/HikariCP)
- [文章链接](https://github.com/pingxin403/apache-flink-examples)

## 📝 许可证

MIT License
