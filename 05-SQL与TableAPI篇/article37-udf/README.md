# Article 37 - UDF/UDAF/UDTF 开发示例代码

本目录包含《UDF/UDAF/UDTF 开发：函数注册、类型推导、测试方法》文章的配套代码示例。

## 项目结构

```
article37-udf/
├── pom.xml                                          # Maven 依赖配置
├── README.md                                        # 本文件
└── src/main/java/com/example/flink/
    ├── MaskPhoneUdf.java                            # UDF 示例：手机号脱敏
    ├── WeightedAvgUdaf.java                         # UDAF 示例：加权平均
    └── SplitUdtf.java                               # UDTF 示例：字符串拆分
```

## 示例说明

### 1. MaskPhoneUdf - 标量函数（UDF，一对一）

手机号脱敏函数，将 11 位手机号的中间 4 位替换为 `****`。

- **输入**：一个手机号字符串
- **输出**：脱敏后的手机号字符串
- **示例**：`13812345678` → `138****5678`
- **支持重载**：可指定保留前缀和后缀的长度

核心 SQL 用法：
```sql
CREATE TEMPORARY FUNCTION mask_phone AS 'com.example.flink.MaskPhoneUdf';
SELECT user_id, mask_phone(phone) AS masked_phone FROM user_info;
```

### 2. WeightedAvgUdaf - 聚合函数（UDAF，多对一）

加权平均函数，计算 `SUM(value * weight) / SUM(weight)`。

- **输入**：多行数据（值和权重）
- **输出**：一个加权平均值
- **累加器**：存储 `weightedSum` 和 `weightSum` 两个中间值
- **支持 Retract**：实现了 `retract()` 方法，支持撤回场景
- **支持 Merge**：实现了 `merge()` 方法，支持 Session 窗口

核心 SQL 用法：
```sql
CREATE TEMPORARY FUNCTION weighted_avg AS 'com.example.flink.WeightedAvgUdaf';
SELECT category, weighted_avg(price, sales_volume) AS avg_price
FROM products GROUP BY category;
```

### 3. SplitUdtf - 表值函数（UDTF，一对多）

字符串拆分函数，按分隔符将字符串拆分为多行，每行包含单词和位置索引。

- **输入**：一个字符串和分隔符
- **输出**：多行数据（word, pos）
- **示例**：`split_tags("Java,Flink,Kafka", ",")` → 3 行
- **支持默认分隔符**：不传分隔符时默认使用逗号

核心 SQL 用法：
```sql
CREATE TEMPORARY FUNCTION split_tags AS 'com.example.flink.SplitUdtf';
SELECT user_id, tag.word, tag.pos
FROM user_tags,
     LATERAL TABLE(split_tags(tags, ',')) AS tag(word, pos);
```

## 三种函数类型对比

| 函数类型 | 类名 | 输入输出 | 继承基类 | 核心方法 |
|:---|:---|:---:|:---|:---|
| UDF | MaskPhoneUdf | 一对一 | `ScalarFunction` | `eval()` |
| UDAF | WeightedAvgUdaf | 多对一 | `AggregateFunction` | `createAccumulator()` / `accumulate()` / `getValue()` |
| UDTF | SplitUdtf | 一对多 | `TableFunction` | `eval()` + `collect()` |

## 运行环境

- JDK 11+
- Apache Flink 1.17.2
- Maven 3.6+

## 编译方式

```bash
# 编译项目
mvn clean compile

# 打包（生成可部署的 JAR）
mvn clean package
```

## 函数注册方式

```sql
-- 方式一：临时函数（仅当前会话有效）
CREATE TEMPORARY FUNCTION mask_phone AS 'com.example.flink.MaskPhoneUdf';

-- 方式二：持久化函数（存储到 Catalog，跨会话可用）
CREATE FUNCTION my_catalog.my_db.mask_phone AS 'com.example.flink.MaskPhoneUdf';

-- 方式三：动态加载 JAR（Flink 1.17+ 支持）
ADD JAR '/path/to/article37-udf-1.0-SNAPSHOT.jar';
CREATE TEMPORARY FUNCTION mask_phone AS 'com.example.flink.MaskPhoneUdf';
```

## 类型推导注解说明

| 注解 | 作用 | 使用位置 |
|:---|:---|:---|
| `@DataTypeHint("STRING")` | 标注单个参数或返回值类型 | eval 方法参数 |
| `@FunctionHint(output = ...)` | 标注函数整体输出类型 | 类级别 |

## GitHub 仓库

完整代码请参考：[https://github.com/pingxin403/apache-flink-examples](https://github.com/pingxin403/apache-flink-examples)

## 相关文章

- [第 37 篇：UDF/UDAF/UDTF 开发](../../../Flink/05-SQL与TableAPI篇/37-UDF开发.md)
- [Flink 官方文档：User-defined Functions](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/udfs/)
- [Flink 官方文档：Data Types](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/types/)
