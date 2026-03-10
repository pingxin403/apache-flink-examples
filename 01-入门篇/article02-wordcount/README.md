# Flink 流式 WordCount 示例

> 本示例对应「Apache Flink 从入门到专家」系列第 02 篇：《🚀 5 分钟跑通你的第一个 Flink 作业：流式 WordCount 全流程》

## 项目简介

本项目演示了如何使用 Apache Flink 实现一个简单的流式 WordCount 程序，实时统计从 Socket 接收的文本中每个单词出现的次数。

## 功能特性

- ✅ 实时接收 Socket 数据流
- ✅ 按空格切分单词
- ✅ 实时统计每个单词出现的次数
- ✅ 结果持续更新，永不停止
- ✅ 提供两种实现方式：匿名内部类和 Lambda 表达式

## 环境要求

- JDK 8 或 JDK 11（推荐 JDK 11）
- Maven 3.x
- Apache Flink 1.17.2

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 启动 Socket 服务器

在终端启动一个 Socket 服务器（模拟数据源）：

```bash
nc -lk 9999
```

如果你的系统没有 `nc` 命令，可以使用 Python：

```python
# socket_server.py
import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)
print("Socket server listening on port 9999...")

conn, addr = server.accept()
print(f"Connected by {addr}")

while True:
    data = input("Enter text: ")
    conn.sendall((data + "\n").encode())
```

### 3. 运行 Flink 作业

#### 方式一：使用 IDE

在 IntelliJ IDEA 中右键运行 `WordCount.main()` 或 `WordCountLambda.main()`

#### 方式二：使用 Maven

```bash
mvn exec:java -Dexec.mainClass=com.example.flink.WordCount
```

或者运行 Lambda 版本：

```bash
mvn exec:java -Dexec.mainClass=com.example.flink.WordCountLambda
```

### 4. 输入数据，观察结果

在 Socket 终端输入：

```
hello world
hello flink
flink is awesome
```

Flink 控制台会实时输出：

```
(hello,1)
(world,1)
(hello,2)
(flink,1)
(flink,2)
(is,1)
(awesome,1)
```

## 代码结构

```
src/main/java/com/example/flink/
├── WordCount.java          # 使用匿名内部类实现
└── WordCountLambda.java    # 使用 Lambda 表达式实现
```

## 核心概念

### DataStream API 三要素

1. **Source（数据源）**：`env.socketTextStream("localhost", 9999)`
   - 从 Socket 读取数据流

2. **Transformation（转换）**：
   - `flatMap`：将每行文本切分成单词
   - `keyBy`：按单词分组
   - `sum`：对计数求和

3. **Sink（输出）**：`wordCounts.print()`
   - 将结果输出到控制台

### 关键算子说明

| 算子 | 功能 | 输入 | 输出 |
|------|------|------|------|
| `flatMap` | 一对多转换 | 一行文本 | 多个 (单词, 1) 元组 |
| `keyBy` | 按 key 分组 | (单词, 1) | 相同单词路由到同一实例 |
| `sum` | 聚合求和 | (单词, 1) | (单词, 累计次数) |

## 常见问题

### Q1: 连接被拒绝（Connection Refused）

**错误信息**：
```
java.net.ConnectException: Connection refused
```

**解决方案**：
1. 确保 Socket 服务器已启动：`nc -lk 9999`
2. 检查端口号是否一致（代码中是 9999）

### Q2: Lambda 表达式报错

**错误信息**：
```
The generic type parameters of 'Collector' are missing.
```

**解决方案**：
使用 Lambda 时必须通过 `.returns()` 显式指定返回类型：

```java
.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
    // ...
})
.returns(Types.TUPLE(Types.STRING, Types.INT))  // 必须添加这一行
```

### Q3: 中文乱码

**解决方案**：
在代码开头添加：

```java
System.setProperty("file.encoding", "UTF-8");
```

## 扩展练习

1. **修改分隔符**：将空格改为逗号或其他字符
2. **统计单词长度**：输出 (单词, 长度) 而不是 (单词, 次数)
3. **过滤短单词**：只统计长度 >= 3 的单词
4. **大小写不敏感**：将所有单词转为小写后再统计

## 参考资料

- [Apache Flink 官方文档 - DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/)
- [Flink 本地调试指南](https://flink.apache.org/2020/02/11/debugging-flink-applications.html)
- [DataStream API 常用算子详解](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/)

## 作者

韩云朋 - 后端开发工程师，CKA 认证

## 许可证

本项目采用 Apache License 2.0 许可证
