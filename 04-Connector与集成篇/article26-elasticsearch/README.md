# Article 26: Elasticsearch 实时索引

本项目演示如何使用 Flink 将数据实时写入 Elasticsearch，涵盖批量写入、重试机制、动态索引等核心功能。

## 项目结构

```
article26-elasticsearch/
├── pom.xml                                    # Maven 配置文件
├── README.md                                  # 项目说明文档
└── src/main/java/com/example/flink/
    ├── UserBehavior.java                      # 用户行为数据模型
    ├── UserBehaviorSource.java                # 模拟数据源
    ├── ElasticsearchSinkExample.java          # 完整示例（推荐）
    ├── BulkProcessorConfigExample.java        # 批量写入配置示例
    └── DynamicIndexExample.java               # 动态索引示例
```

## 核心功能

### 1. ElasticsearchSinkExample（完整示例）

演示 Flink Elasticsearch Sink 的完整配置，包括：
- 批量写入参数配置（MaxActions、MaxSizeMb、Interval）
- 重试机制配置（指数退避）
- 动态索引名称（按天分索引）
- 自定义失败处理策略

### 2. BulkProcessorConfigExample（批量写入配置）

演示不同场景下的批量写入参数配置：
- **高吞吐场景**：大批次、长间隔（适合日志、埋点）
- **低延迟场景**：小批次、短间隔（适合实时搜索、监控）
- **均衡场景**：中等批次、适中间隔（适合大多数业务）

### 3. DynamicIndexExample（动态索引）

演示如何根据数据内容动态生成索引名称：
- 按日期分索引：`user_behavior_2025_01_15`
- 按类目分索引：`user_behavior_电子产品`
- 按日期+类目分索引：`user_behavior_2025_01_15_电子产品`

## 环境要求

- Java 11+
- Apache Flink 1.17.2
- Elasticsearch 7.17.9
- Maven 3.6+

## 运行前准备

### 1. 启动 Elasticsearch

使用 Docker 快速启动：

```bash
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  elasticsearch:7.17.9
```

验证 ES 是否启动成功：

```bash
curl http://localhost:9200
```

### 2. 创建索引模板（可选）

为了统一管理索引配置，建议创建索引模板：

```bash
curl -X PUT "localhost:9200/_index_template/user_behavior_template" \
  -H 'Content-Type: application/json' \
  -d '{
  "index_patterns": ["user_behavior_*"],
  "template": {
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1,
      "refresh_interval": "30s"
    },
    "mappings": {
      "properties": {
        "user_id": { "type": "keyword" },
        "action": { "type": "keyword" },
        "product_id": { "type": "keyword" },
        "category": { "type": "keyword" },
        "timestamp": { "type": "date" }
      }
    }
  }
}'
```

## 编译和运行

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例

#### 方式一：IDE 中运行

直接运行 `ElasticsearchSinkExample.main()` 方法。

#### 方式二：命令行运行

```bash
# 运行完整示例
mvn exec:java -Dexec.mainClass="com.example.flink.ElasticsearchSinkExample"

# 运行批量写入配置示例
mvn exec:java -Dexec.mainClass="com.example.flink.BulkProcessorConfigExample"

# 运行动态索引示例
mvn exec:java -Dexec.mainClass="com.example.flink.DynamicIndexExample"
```

#### 方式三：提交到 Flink 集群

```bash
flink run -c com.example.flink.ElasticsearchSinkExample \
  target/article26-elasticsearch-1.0-SNAPSHOT.jar
```

## 验证结果

### 1. 查看索引列表

```bash
curl http://localhost:9200/_cat/indices?v
```

你应该能看到类似 `user_behavior_2025_01_15` 的索引。

### 2. 查询数据

```bash
# 查询指定索引的数据
curl -X GET "localhost:9200/user_behavior_2025_01_15/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
  "query": {
    "match_all": {}
  },
  "size": 10
}'

# 按用户ID查询
curl -X GET "localhost:9200/user_behavior_*/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
  "query": {
    "term": {
      "user_id": "user_123"
    }
  }
}'

# 按行为类型聚合
curl -X GET "localhost:9200/user_behavior_*/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
  "size": 0,
  "aggs": {
    "action_count": {
      "terms": {
        "field": "action"
      }
    }
  }
}'
```

### 3. 监控写入性能

```bash
# 查看索引统计信息
curl http://localhost:9200/user_behavior_*/_stats?pretty

# 查看批量操作统计
curl http://localhost:9200/_nodes/stats/indices/indexing?pretty
```

## 配置说明

### 批量写入参数

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `setBulkFlushMaxActions` | 每批最大记录数 | 1000-10000 |
| `setBulkFlushMaxSizeMb` | 每批最大字节数（MB） | 5-20 |
| `setBulkFlushInterval` | 批次刷新间隔（毫秒） | 1000-30000 |

### 重试策略参数

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `setBulkFlushBackoffType` | 退避类型（CONSTANT/EXPONENTIAL） | EXPONENTIAL |
| `setBulkFlushBackoffRetries` | 最大重试次数 | 3-5 |
| `setBulkFlushBackoffDelay` | 初始延迟（毫秒） | 1000 |

## 常见问题

### 1. 连接 ES 失败

**错误信息**：`Connection refused`

**解决方案**：
- 检查 ES 是否启动：`curl http://localhost:9200`
- 检查防火墙设置
- 确认 ES 地址和端口配置正确

### 2. 数据写入慢

**现象**：Flink 作业出现反压

**解决方案**：
- 增大批量写入参数（MaxActions、MaxSizeMb）
- 增加 Flink 并行度
- 调整 ES 的 `refresh_interval`（默认 1s，可调大到 30s）
- 扩容 ES 集群

### 3. 索引数量过多

**现象**：ES 集群不稳定，查询变慢

**解决方案**：
- 调整索引粒度（从按小时改为按天）
- 使用 ILM（Index Lifecycle Management）自动清理历史索引
- 使用 Rollover API 控制索引大小

## 性能优化建议

1. **调整 refresh_interval**：从默认的 1s 调大到 30s，可提升写入吞吐 30%-50%
2. **禁用副本**：初始导入时临时禁用副本，导入完成后再恢复
3. **使用 _id 幂等**：指定唯一 ID，避免重复数据
4. **监控关键指标**：关注 Flink 的 `numRecordsOut` 和 ES 的 `indexing.index_time_in_millis`

## 相关文章

- 第 24 篇：Kafka + Flink Exactly-Once
- 第 25 篇：JDBC Sink 写 MySQL
- 第 27 篇：异步 I/O 不阻塞

## 参考资料

- [Flink Elasticsearch Connector 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/elasticsearch/)
- [Elasticsearch Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- [Elasticsearch 性能调优指南](https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html)
