# Apache Flink 从入门到专家 — 代码示例仓库

> 本仓库是「**Apache Flink 从入门到专家**」67 篇系列文章的配套代码示例集合。
> 每个示例均为独立的 Maven 项目，可直接编译运行。

📖 系列文章作者：**韩云朋**（后端开发工程师，CKA 认证）

🔗 GitHub 仓库地址：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

---

## 一、仓库简介

本仓库收录了系列文章中涉及的所有可运行代码示例，覆盖 DataStream API、State 管理、窗口计算、Connector 集成、Flink SQL、性能优化、实时数仓等核心主题。代码按照文章所属模块分目录组织，方便读者对照文章动手实践。

---

## 二、环境要求

| 依赖项 | 最低版本 | 说明 |
| :--- | :---: | :--- |
| JDK | 11+ | 推荐使用 JDK 11 或 JDK 17 |
| Maven | 3.6+ | 用于编译和打包 |
| Apache Flink | 1.17+ | 部分示例需要本地 Flink 集群 |
| Git | 2.x | 克隆仓库 |

> 💡 大部分示例可在本地 IDE（IntelliJ IDEA / VS Code）中直接运行 `main` 方法，无需部署 Flink 集群。

---

## 三、快速开始

### 3.1 克隆仓库

```bash
git clone https://github.com/pingxin403/apache-flink-examples.git
cd apache-flink-examples
```

### 3.2 编译单个示例

每个示例是独立的 Maven 项目，进入对应目录后执行：

```bash
cd 01-入门篇/article02-wordcount
mvn clean compile
```

### 3.3 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.WordCount"
```

或在 IDE 中直接运行对应的 `main` 方法。

### 3.4 打包提交到集群

```bash
mvn clean package -DskipTests
# 生成的 JAR 位于 target/ 目录下
flink run target/xxx.jar
```

---

## 四、代码示例目录

### 模块一：入门篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `01-入门篇/article02-wordcount/` | 第 02 篇：5 分钟跑通你的第一个 Flink 作业 | WordCount 入门示例 |
| `01-入门篇/article04-datastream-operators/` | 第 04 篇：DataStream 三要素实战拆解 | map / filter / keyBy / window 算子 |
| `01-入门篇/article07-windows/` | 第 07 篇：窗口初体验 | Tumbling / Sliding / Session 窗口 |

### 模块二：State 与容错篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `02-State与容错篇/article10-keyed-state/` | 第 10 篇：Keyed State 四剑客 | ValueState / ListState / MapState / ReducingState |
| `02-State与容错篇/article11-operator-state/` | 第 11 篇：Operator State 实战 | BroadcastState / ListState / Checkpoint 集成 |
| `02-State与容错篇/article15-state-ttl/` | 第 15 篇：状态也会"过期"？TTL 自动清理 | State TTL 配置与过期策略 |

### 模块三：时间与窗口篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `03-时间与窗口篇/article17-watermark-strategies/` | 第 17 篇：Watermark 生成策略 | 周期性 / 标点式水印、空闲源处理 |
| `03-时间与窗口篇/article18-late-data/` | 第 18 篇：迟到数据别丢！ | Allowed Lateness + Side Output |
| `03-时间与窗口篇/article19-window-functions/` | 第 19 篇：窗口函数进化史 | Reduce / Aggregate / ProcessWindowFunction |
| `03-时间与窗口篇/article20-custom-trigger/` | 第 20 篇：自定义触发器 | Trigger 实现与动态窗口行为 |
| `03-时间与窗口篇/article21-session-window/` | 第 21 篇：会话窗口实战 | 用户行为 Session 分析 |
| `03-时间与窗口篇/article22-topn/` | 第 22 篇：实时排行榜怎么做？ | 窗口 TOP-N 实现 |
| `03-时间与窗口篇/article23-cumulate-window/` | 第 23 篇：累积窗口 | Cumulate Window SQL 实现 |

### 模块四：Connector 与集成篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `04-Connector与集成篇/article24-kafka-exactly-once/` | 第 24 篇：Kafka + Flink Exactly-Once | 事务协调、动态分区发现 |
| `04-Connector与集成篇/article25-jdbc-sink/` | 第 25 篇：JDBC Sink 写 MySQL | 批量写入、连接池、幂等控制 |
| `04-Connector与集成篇/article26-elasticsearch/` | 第 26 篇：Elasticsearch 实时索引 | BulkProcessor、重试机制 |
| `04-Connector与集成篇/article27-async-io/` | 第 27 篇：异步 I/O 不阻塞 | AsyncFunction、并发控制 |
| `04-Connector与集成篇/article28-flink-cdc/` | 第 28 篇：Flink CDC 实战 | MySQL 全量 + 增量同步 |
| `04-Connector与集成篇/article30-custom-source/` | 第 30 篇：自定义 Source 开发 | Checkpoint 集成、状态恢复 |
| `04-Connector与集成篇/article31-custom-sink/` | 第 31 篇：自定义 Sink 开发 | TwoPhaseCommitSinkFunction |

### 模块五：SQL 与 Table API 篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `05-SQL与TableAPI篇/article32-table-api-intro/` | 第 32 篇：Table API 入门 | 环境配置、流表互转 |
| `05-SQL与TableAPI篇/article33-sql-windows/` | 第 33 篇：流式 SQL 窗口 | Tumble / Hop / Session SQL 语法 |
| `05-SQL与TableAPI篇/article34-lookup-join/` | 第 34 篇：维表 Join 性能差？ | Lookup Join + 缓存策略优化 |
| `05-SQL与TableAPI篇/article35-stream-join/` | 第 35 篇：双流 Join 怎么做？ | Window Join / Interval Join |
| `05-SQL与TableAPI篇/article36-dedup-topn/` | 第 36 篇：实时去重 & TOP-N | ROW_NUMBER / DISTINCT |
| `05-SQL与TableAPI篇/article37-udf/` | 第 37 篇：UDF/UDAF/UDTF 开发 | 函数注册、类型推导 |

### 模块七：性能优化篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `07-性能优化篇/article47-data-skew/` | 第 47 篇：数据倾斜怎么办？ | Salting + 两阶段聚合 |
| `07-性能优化篇/article51-mini-batch/` | 第 51 篇：Mini-Batch 聚合 | 降低状态更新频率、提升吞吐 |

### 模块八：实时数仓篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `08-实时数仓篇/article53-realtime-wide-table/` | 第 53 篇：实时宽表构建 | 流式 Join + 维表关联 |
| `08-实时数仓篇/article54-uv-pv/` | 第 54 篇：UV/PV/留存率 | 实时指标计算 |

### 模块九：专家进阶篇

| 目录 | 对应文章 | 关键内容 |
| :--- | :--- | :--- |
| `09-专家进阶篇/article58-e2e-exactly-once/` | 第 58 篇：端到端 Exactly-Once | Kafka → Flink → Kafka 全链路保障 |

> 📝 **说明**：模块六（部署与运维篇）和模块十（避坑与成长篇）的文章以概念讲解和运维实践为主，不包含独立的代码示例项目。

---

## 五、项目结构总览

```
apache-flink-examples/
├── 01-入门篇/
│   ├── article02-wordcount/
│   ├── article04-datastream-operators/
│   └── article07-windows/
├── 02-State与容错篇/
│   ├── article10-keyed-state/
│   ├── article11-operator-state/
│   └── article15-state-ttl/
├── 03-时间与窗口篇/
│   ├── article17-watermark-strategies/
│   ├── article18-late-data/
│   ├── article19-window-functions/
│   ├── article20-custom-trigger/
│   ├── article21-session-window/
│   ├── article22-topn/
│   └── article23-cumulate-window/
├── 04-Connector与集成篇/
│   ├── article24-kafka-exactly-once/
│   ├── article25-jdbc-sink/
│   ├── article26-elasticsearch/
│   ├── article27-async-io/
│   ├── article28-flink-cdc/
│   ├── article30-custom-source/
│   └── article31-custom-sink/
├── 05-SQL与TableAPI篇/
│   ├── article32-table-api-intro/
│   ├── article33-sql-windows/
│   ├── article34-lookup-join/
│   ├── article35-stream-join/
│   ├── article36-dedup-topn/
│   └── article37-udf/
├── 07-性能优化篇/
│   ├── article47-data-skew/
│   └── article51-mini-batch/
├── 08-实时数仓篇/
│   ├── article53-realtime-wide-table/
│   └── article54-uv-pv/
├── 09-专家进阶篇/
│   └── article58-e2e-exactly-once/
└── README.md
```

每个 `articleXX-xxx/` 目录都是一个独立的 Maven 项目，包含：

```
articleXX-xxx/
├── pom.xml                          # Maven 依赖配置
├── src/main/java/com/example/flink/ # Java 源码
│   └── XxxExample.java
└── README.md                        # 示例说明（部分项目）
```

---

## 六、技术栈与版本

| 技术 | 版本 | 用途 |
| :--- | :---: | :--- |
| Apache Flink | 1.17.x | 流处理引擎 |
| Java | 11+ | 开发语言 |
| Maven | 3.6+ | 构建工具 |
| Flink CDC | 2.4+ | 变更数据捕获（部分示例） |
| Kafka | 3.x | 消息队列（部分示例） |
| MySQL | 8.0+ | 数据库（部分示例） |
| Elasticsearch | 7.x | 搜索引擎（部分示例） |

---

## 七、常见问题

**Q：示例能直接在 IDE 中运行吗？**
A：大部分示例可以直接运行 `main` 方法。涉及外部依赖（Kafka、MySQL、Elasticsearch 等）的示例需要先启动对应服务。

**Q：Flink 版本不一致怎么办？**
A：修改对应项目 `pom.xml` 中的 `<flink.version>` 属性即可适配其他版本。

**Q：如何在集群上运行？**
A：执行 `mvn clean package -DskipTests` 打包后，使用 `flink run target/xxx.jar` 提交到集群。

---

## 八、关于系列文章

「**Apache Flink 从入门到专家**」是一套共 67 篇的系统性学习教程，涵盖 10 大模块：

1. **入门篇**（8 篇）— 流式计算基础概念与环境搭建
2. **State 与容错篇**（8 篇）— 状态管理、Checkpoint 与容错机制
3. **时间与窗口篇**（7 篇）— 时间语义、Watermark 与窗口计算
4. **Connector 与集成篇**（8 篇）— Kafka、JDBC、ES、CDC 等外部系统集成
5. **SQL 与 Table API 篇**（7 篇）— 声明式流处理与 UDF 开发
6. **部署与运维篇**（7 篇）— K8s 部署、监控告警与故障排查
7. **性能优化篇**（6 篇）— 数据倾斜、Checkpoint 调优与网络优化
8. **实时数仓篇**（5 篇）— 分层设计、宽表构建与指标计算
9. **专家进阶篇**（5 篇）— 平台建设、源码导读与技术选型
10. **避坑与成长篇**（6 篇）— 常见陷阱、面试题与成长路径

---

## 九、许可证

本仓库代码仅供学习和参考使用。

---

**关于作者**：韩云朋，后端开发工程师，CKA 认证。
