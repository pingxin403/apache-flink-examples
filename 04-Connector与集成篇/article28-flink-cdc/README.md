# Article 28: Flink CDC 实战 —— MySQL 全量+增量同步

本示例演示如何使用 Flink CDC 实现 MySQL 数据的全量+增量实时同步。

## 功能说明

- 使用 Flink CDC MySQL Connector 监听 `orders` 表的数据变更
- 首次启动自动执行全量快照读取（无锁），然后无缝切换到增量 binlog 读取
- 基于 Checkpoint 实现 Exactly-Once 语义
- 演示 CDC 事件的过滤和处理

## 核心特性

### 1. 增量快照算法（无锁全量读取）
- 将大表拆分为多个 Chunk 并行读取
- 通过 binlog 回放保证数据一致性
- 无需对源表加锁，不影响线上业务

### 2. 全量+增量一体化
- `StartupOptions.initial()`: 先全量后增量
- `StartupOptions.latest()`: 只读增量
- `StartupOptions.timestamp()`: 从指定时间点开始

### 3. Exactly-Once 保障
- 依赖 Flink Checkpoint 机制
- 故障恢复后从上次 Checkpoint 位点继续

## 前置条件

### 1. MySQL 配置

确保 MySQL 开启了 binlog：

```sql
-- 检查 binlog 配置
SHOW VARIABLES LIKE 'log_bin';           -- 应为 ON
SHOW VARIABLES LIKE 'binlog_format';     -- 应为 ROW
SHOW VARIABLES LIKE 'binlog_row_image';  -- 应为 FULL
```

如果未开启，在 `my.cnf` 中添加：

```ini
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

### 2. 创建 CDC 专用账号

```sql
CREATE USER 'flink_cdc'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';
FLUSH PRIVILEGES;
```

### 3. 创建测试表

```sql
CREATE DATABASE IF NOT EXISTS mydb;
USE mydb;

CREATE TABLE orders (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(32) NOT NULL,
    product_name VARCHAR(128) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(16) DEFAULT 'CREATED',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO orders (user_id, product_name, amount, status) VALUES
('user_001', 'Flink实战指南', 99.00, 'CREATED'),
('user_002', 'Kafka权威指南', 79.00, 'PAID'),
('user_003', 'Java并发编程', 59.00, 'SHIPPED');
```

### 4. Flink 环境
- Flink 版本 >= 1.17
- Java 11+

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行 Flink 作业

```bash
flink run -c com.example.flink.MysqlCdcExample \
  target/article28-flink-cdc-1.0-SNAPSHOT.jar
```

### 3. 模拟数据变更

在 MySQL 中执行以下操作，观察 Flink 控制台输出：

```sql
-- INSERT 事件
INSERT INTO orders (user_id, product_name, amount, status)
VALUES ('user_004', '深入理解JVM', 69.00, 'CREATED');

-- UPDATE 事件
UPDATE orders SET status = 'PAID' WHERE id = 1;

-- DELETE 事件（会被过滤器过滤掉）
DELETE FROM orders WHERE id = 3;
```

### 4. 预期输出

```
CDC Event> [SNAPSHOT] {"before":null,"after":{"id":1,"user_id":"user_001",...},"op":"r",...}
CDC Event> [SNAPSHOT] {"before":null,"after":{"id":2,"user_id":"user_002",...},"op":"r",...}
CDC Event> [INSERT]   {"before":null,"after":{"id":4,"user_id":"user_004",...},"op":"c",...}
CDC Event> [UPDATE]   {"before":{"id":1,...,"status":"CREATED"},"after":{"id":1,...,"status":"PAID"},"op":"u",...}
```

## 项目结构

```
article28-flink-cdc/
├── pom.xml                          # Maven 依赖配置
├── README.md                        # 项目说明
└── src/main/java/com/example/flink/
    ├── MysqlCdcExample.java         # CDC 主程序
    └── Order.java                   # 订单 POJO
```

## 配置说明

### Checkpoint 配置

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `checkpointInterval` | 60000 | Checkpoint 间隔（毫秒） |
| `checkpointingMode` | EXACTLY_ONCE | 精确一次语义 |
| `checkpointTimeout` | 600000 | 超时时间（全量阶段可能较慢） |
| `minPauseBetweenCheckpoints` | 5000 | 两次 Checkpoint 最小间隔 |

### CDC Source 配置

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `splitSize` | 8096 | Chunk 大小，大表可调大到 100000 |
| `fetchSize` | 1024 | 每次拉取行数 |
| `startupOptions` | initial() | 先全量后增量 |

## 常见问题

### 1. 连接 MySQL 失败

确保 MySQL 用户有 `REPLICATION SLAVE` 和 `REPLICATION CLIENT` 权限。

### 2. 全量阶段特别慢

调大 `splitSize` 参数，减少 Chunk 数量。

### 3. binlog 被清理

调大 MySQL 的 `expire_logs_days` 参数（建议 >= 7 天）。

## 参考资料

- [Flink CDC 官方文档](https://ververica.github.io/flink-cdc-connectors/master/)
- [MySQL CDC Connector 配置](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html)
- [Flink Checkpoint 机制](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)

## 相关文章

- [第 13 篇：Checkpoint 背后的 Chandy-Lamport 算法](../../../Flink/02-State与容错篇/13-Checkpoint背后的Chandy-Lamport算法.md)
- [第 28 篇：Flink CDC 实战](../../../Flink/04-Connector与集成篇/28-Flink%20CDC实战.md)
