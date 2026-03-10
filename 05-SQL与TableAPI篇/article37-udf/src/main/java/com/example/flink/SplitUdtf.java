package com.example.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 字符串拆分 UDTF（表值函数）
 *
 * <p>功能：按指定分隔符将字符串拆分为多行，每行包含拆分后的单词及其位置索引。
 * <p>示例：split_tags("Java,Flink,Kafka", ",") → 3 行：(Java,0), (Flink,1), (Kafka,2)
 *
 * <p>UDTF 处理一行输入，输出多行结果（一对多映射）。
 * 开发步骤：
 * 1. 继承 TableFunction&lt;T&gt;，T 是输出行的类型
 * 2. 使用 @FunctionHint 标注输出类型（尤其是 Row 类型时必须标注）
 * 3. 实现 eval 方法，在方法内通过 collect() 输出多行
 *
 * <p>SQL 使用方式（通过 LATERAL TABLE 语法调用）：
 * <pre>
 * CREATE TEMPORARY FUNCTION split_tags AS 'com.example.flink.SplitUdtf';
 * SELECT user_id, tag.word, tag.pos
 * FROM user_tags,
 *      LATERAL TABLE(split_tags(tags, ',')) AS tag(word, pos);
 * </pre>
 *
 * <p>输入输出示例：
 * <pre>
 * 输入：(user_id=1, tags="Java,Flink,Kafka")
 * 输出：
 *   (user_id=1, word="Java",  pos=0)
 *   (user_id=1, word="Flink", pos=1)
 *   (user_id=1, word="Kafka", pos=2)
 * </pre>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/udfs/#table-functions">
 *     Flink Table Function 官方文档</a>
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, pos INT>"))
public class SplitUdtf extends TableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(SplitUdtf.class);

    /**
     * 按指定分隔符拆分字符串，输出多行结果。
     *
     * <p>每行包含两个字段：
     * - word：拆分后的单词（已去除首尾空格）
     * - pos：该单词在原字符串中的位置索引（从 0 开始）
     *
     * <p>边界处理：
     * - 输入为 null 时，不输出任何行
     * - 分隔符为 null 时，不输出任何行
     * - 拆分后的空字符串会被跳过
     *
     * @param str       待拆分的字符串
     * @param separator 分隔符（支持正则表达式）
     */
    public void eval(String str, String separator) {
        if (str == null || separator == null) {
            return;  // 空输入不输出任何行
        }
        try {
            String[] parts = str.split(separator, -1);
            for (int i = 0; i < parts.length; i++) {
                String word = parts[i].trim();
                if (!word.isEmpty()) {
                    // collect() 是 TableFunction 的核心方法，用于输出一行数据
                    collect(Row.of(word, i));
                }
            }
        } catch (Exception e) {
            LOG.warn("字符串拆分失败, str={}, separator={}", str, separator, e);
        }
    }

    /**
     * 重载方法：使用默认分隔符（逗号）拆分字符串。
     *
     * @param str 待拆分的字符串
     */
    public void eval(String str) {
        eval(str, ",");
    }
}
