# Article 10: Keyed State 四剑客

本项目演示 Flink 四种 Keyed State 的使用场景和 API。

## 项目结构

```
article10-keyed-state/
├── src/main/java/com/example/flink/
│   ├── model/
│   │   ├── LoginEvent.java          # 登录事件数据模型
│   │   └── Order.java                # 订单数据模型
│   ├── ValueStateExample.java        # ValueState 示例：登录监控
│   ├── ListStateExample.java         # ListState 示例：购买记录追踪
│   ├── MapStateExample.java          # MapState 示例：商品销量分析
│   ├── ReducingStateExample.java     # ReducingState 示例：消费累计
│   └── AllStatesExample.java         # 综合示例：用户行为分析
├── pom.xml
└── README.md
```

## 四种状态类型对比

| 状态类型 | 存储结构 | 典型场景 | 示例程序 |
|---------|---------|---------|---------|
| **ValueState<T>** | 单个值 | 记录最新值、标记位 | ValueStateExample |
| **ListState<T>** | 列表 | 历史记录、窗口缓存 | ListStateExample |
| **MapState<K,V>** | 键值对 | 分组统计、多维度聚合 | MapStateExample |
| **ReducingState<T>** | 单个聚合值 | 累加、求和、求最大值 | ReducingStateExample |

## 运行示例

### 1. ValueState 示例：登录监控

监控用户频繁登录行为，1小时内重复登录则告警。

```bash
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.flink.ValueStateExample"
```

**预期输出**：
```
⚠️ 用户 user1 频繁登录告警：间隔仅 1234 秒 (IP: 192.168.1.1)
```

**核心代码**：
```java
// 定义状态
ValueState<Long> lastLoginTimeState;

// 读取状态
Long lastLoginTime = lastLoginTimeState.value();

// 更新状态
lastLoginTimeState.update(currentTime);
```

### 2. ListState 示例：购买记录追踪

记录每个用户最近10次购买，分析购买偏好。

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.ListStateExample"
```

**预期输出**：
```
📊 用户 user1 购买分析：最近10次购买，偏好品类=电子产品，平均消费=1234.56元
```

**核心代码**：
```java
// 定义状态
ListState<Order> recentOrdersState;

// 读取状态
List<Order> orders = new ArrayList<>();
for (Order o : recentOrdersState.get()) {
    orders.add(o);
}

// 更新状态
recentOrdersState.clear();
recentOrdersState.addAll(orders);
```

### 3. MapState 示例：商品销量分析

统计每个商品在各城市的实时销量分布。

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.MapStateExample"
```

**预期输出**：
```
📈 商品 iPhone15 销量分布：
  北京: 10件 (25.0%)
  上海: 15件 (37.5%)
  广州: 8件 (20.0%)
  深圳: 7件 (17.5%)
  总销量: 40件
```

**核心代码**：
```java
// 定义状态
MapState<String, Integer> citySalesState;

// 读取单个值
Integer sales = citySalesState.get(city);

// 更新值
citySalesState.put(city, sales + quantity);

// 遍历所有键值对
for (Map.Entry<String, Integer> entry : citySalesState.entries()) {
    // 处理每个城市的销量
}
```

### 4. ReducingState 示例：消费累计

计算用户累计消费金额，达到VIP标准时触发升级。

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.ReducingStateExample"
```

**预期输出**：
```
💰 用户 Alice 新增消费 1234.56元，累计消费 8765.43元 (距离VIP还差 1234.57元)
💰 用户 Alice 新增消费 2000.00元，累计消费 10765.43元 ⭐ 已达到VIP标准！
```

**核心代码**：
```java
// 定义状态（需要提供 ReduceFunction）
ReducingState<Double> totalSpendingState;

// 添加值（自动累加）
totalSpendingState.add(order.getAmount());

// 读取聚合结果
Double totalSpending = totalSpendingState.get();
```

### 5. 综合示例：用户行为分析

同时使用四种状态类型，进行全方位用户行为分析。

```bash
mvn exec:java -Dexec.mainClass="com.example.flink.AllStatesExample"
```

**预期输出**：
```
============================================================
👤 用户 Alice 行为分析报告
============================================================
📦 本次购买：iPhone (5678.90元) - 距上次购买 15 分钟
🛒 最近购买：MacBook → iPad → AirPods → Watch → iPhone
❤️ 偏好品类：电子产品 (购买5次)
💰 累计消费：23456.78元
🏆 会员等级：🌟 黄金会员
============================================================
```

## 状态使用最佳实践

### 1. 选择合适的状态类型

```java
// ✅ 正确：记录单个值用 ValueState
ValueState<Long> lastTimeState;

// ✅ 正确：需要随机访问用 MapState
MapState<String, Integer> cityCountState;

// ❌ 错误：用 ListState 做随机访问（性能差）
ListState<CityCount> cityCountState;  // 查找需要遍历整个列表
```

### 2. 防止状态无限增长

```java
// ❌ 错误：状态一直增长
listState.add(element);  // 从不清理

// ✅ 正确：限制状态大小
List<Element> elements = new ArrayList<>();
for (Element e : listState.get()) {
    elements.add(e);
}
elements.add(newElement);
if (elements.size() > MAX_SIZE) {
    elements.remove(0);  // 移除最旧的
}
listState.update(elements);
```

### 3. 正确初始化状态

```java
// ❌ 错误：忘记初始化
private ValueState<Integer> countState;  // 没有初始化

public void processElement(...) {
    countState.update(1);  // NullPointerException!
}

// ✅ 正确：在 open() 方法中初始化
@Override
public void open(Configuration parameters) {
    ValueStateDescriptor<Integer> descriptor = 
        new ValueStateDescriptor<>("count", Integer.class);
    countState = getRuntimeContext().getState(descriptor);
}
```

### 4. 处理状态为 null 的情况

```java
// ❌ 错误：直接使用状态值
Integer count = countState.value();
count++;  // NullPointerException if count is null

// ✅ 正确：检查 null
Integer count = countState.value();
if (count == null) {
    count = 0;
}
count++;
countState.update(count);
```

## 性能对比

| 操作 | ValueState | ListState | MapState | ReducingState |
|------|-----------|-----------|----------|---------------|
| 读取 | O(1) | O(n) | O(1) | O(1) |
| 写入 | O(1) | O(1) | O(1) | O(1) |
| 查找 | - | O(n) | O(1) | - |
| 遍历 | - | O(n) | O(n) | - |

## 依赖说明

- **Flink 版本**: 1.17.2
- **Java 版本**: 11+
- **构建工具**: Maven 3.6+

## 相关文章

- 第 09 篇：[无状态流处理是玩具？为什么真实业务必须用 State](../../Flink/02-State与容错篇/09-无状态流处理是玩具.md)
- 第 11 篇：Operator State 实战：Source 并行度变化时如何保状态？

## 参考资料

- [Flink State 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
- [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)

## GitHub 仓库

完整代码请访问：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)
