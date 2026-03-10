# Article 33 - 流式 SQL 窗口：Tumble、Hop、Session 语法实战

本项目是《Apache Flink 从入门到专家》系列第 33 篇的配套代码示例，演示 Flink SQL 中三种窗口的使用方法。

## 项目结构

```
article33-sql-windows/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── TumbleWindowExample.java    # Tumble（滚动）窗口示例
    ├── HopWindowExample.java       # Hop（滑动）窗口示例
    └── SessionWindowExample.java   # Session（会话）窗口示例
```

## 环境要求

- JDK 11+
- Maven 3.6+
- Apache Flink 1.17.2

## 运行方式

```bash
# 编译项目
mvn clean package

# 运行 Tumble 窗口示例（每分钟订单统计）
mvn exec:java -Dexec.mainClass="com.example.flink.TumbleWindowExample"

# 运行 Hop 窗口示例（滑动平均订单金额）
mvn exec:java -Dexec.mainClass="com.example.flink.HopWindowExample"

# 运行 Session 窗口示例（用户会话分析）
mvn exec:java -Dexec.mainClass="com.example.flink.SessionWindowExample"
```

## 示例说明

| 示例 | 窗口类型 | 场景 | 语法 |
|:---|:---|:---|:---|
| TumbleWindowExample | 滚动窗口 | 每分钟订单量/金额统计 | TVF（推荐） |
| HopWindowExample | 滑动窗口 | 最近 1 分钟平均金额，每 30 秒更新 | TVF（推荐） |
| SessionWindowExample | 会话窗口 | 用户会话页面浏览统计 | GROUP WINDOW（稳定） |

## 关键知识点

1. **TVF 语法**（Flink 1.13+ 推荐）：窗口函数写在 `FROM TABLE(...)` 中，支持更多优化
2. **GROUP WINDOW 语法**（旧版）：窗口函数写在 `GROUP BY` 中，Session 窗口建议使用此语法
3. **WATERMARK**：事件时间窗口必须声明水位线，否则窗口无法触发
4. **DataGen 连接器**：用于生成模拟数据，方便本地测试

## 相关文章

- [第 33 篇：流式 SQL 窗口](https://github.com/pingxin403/apache-flink-examples)
- [第 32 篇：Table API 入门](../article32-table-api-intro/)
