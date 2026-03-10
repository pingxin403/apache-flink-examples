package com.example.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 加权平均 UDAF（聚合函数）
 *
 * <p>功能：计算加权平均值 = SUM(value * weight) / SUM(weight)
 * <p>示例：weighted_avg(price, sales_volume) 计算按销量加权的平均价格
 *
 * <p>UDAF 处理多行输入，输出一个聚合结果（多对一映射）。
 * 开发步骤：
 * 1. 继承 AggregateFunction&lt;T, ACC&gt;，T 是输出类型，ACC 是累加器类型
 * 2. 定义累加器类（存储中间状态）
 * 3. 实现 createAccumulator()：创建累加器实例
 * 4. 实现 accumulate(ACC, ...)：将一行数据累加到累加器
 * 5. 实现 getValue(ACC)：从累加器提取最终结果
 * 6. （可选）实现 retract()：支持 Retract 场景
 * 7. （可选）实现 merge()：支持 Session 窗口合并
 *
 * <p>SQL 使用方式：
 * <pre>
 * CREATE TEMPORARY FUNCTION weighted_avg AS 'com.example.flink.WeightedAvgUdaf';
 * SELECT category, weighted_avg(price, sales_volume) AS avg_price
 * FROM products GROUP BY category;
 * </pre>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/udfs/#aggregate-functions">
 *     Flink Aggregate Function 官方文档</a>
 */
@FunctionHint(output = @DataTypeHint("DOUBLE"))
public class WeightedAvgUdaf extends AggregateFunction<Double, WeightedAvgUdaf.WeightedAvgAccum> {

    /**
     * 累加器（Accumulator）：存储聚合的中间状态。
     *
     * <p>累加器会被 Flink 序列化到状态后端（如 RocksDB），
     * 因此字段应尽量轻量，避免使用复杂对象。
     *
     * <p>本累加器存储两个值：
     * - weightedSum：加权和 = Σ(value_i * weight_i)
     * - weightSum：权重和 = Σ(weight_i)
     */
    public static class WeightedAvgAccum {
        /** 加权和：所有 value * weight 的累加值 */
        public double weightedSum = 0.0;
        /** 权重和：所有 weight 的累加值 */
        public double weightSum = 0.0;
    }

    /**
     * 创建累加器实例。
     * 每个分组（GROUP BY 的每个 key）会创建一个独立的累加器。
     */
    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    /**
     * 累加方法：每来一行数据，更新累加器中的加权和与权重和。
     *
     * <p>方法名 accumulate 是固定的，Flink 通过反射调用。
     * 参数列表中第一个参数必须是累加器，后续参数对应 SQL 中的函数参数。
     *
     * @param acc    累加器实例
     * @param value  待聚合的值（如价格）
     * @param weight 权重（如销量）
     */
    public void accumulate(WeightedAvgAccum acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.weightedSum += value * weight;
            acc.weightSum += weight;
        }
    }

    /**
     * 撤回方法：在 Retract 流场景中，撤回之前累加的数据。
     *
     * <p>当上游发送 UPDATE_BEFORE（撤回消息）时，Flink 会调用此方法。
     * 如果你的 UDAF 用在 GROUP BY 无窗口的场景中，必须实现此方法。
     *
     * @param acc    累加器实例
     * @param value  待撤回的值
     * @param weight 待撤回的权重
     */
    public void retract(WeightedAvgAccum acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.weightedSum -= value * weight;
            acc.weightSum -= weight;
        }
    }

    /**
     * 合并方法：将多个累加器合并为一个。
     *
     * <p>在 Session 窗口合并或分布式聚合的 Merge 阶段会调用此方法。
     * 如果你的 UDAF 用在 Session 窗口中，必须实现此方法。
     *
     * @param acc       目标累加器
     * @param iterable  待合并的累加器集合
     */
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> iterable) {
        for (WeightedAvgAccum other : iterable) {
            acc.weightedSum += other.weightedSum;
            acc.weightSum += other.weightSum;
        }
    }

    /**
     * 重置累加器：将累加器恢复到初始状态。
     *
     * <p>在某些窗口场景中，Flink 可能会复用累加器对象，
     * 此时需要通过 resetAccumulator 将其重置。
     *
     * @param acc 累加器实例
     */
    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.weightedSum = 0.0;
        acc.weightSum = 0.0;
    }

    /**
     * 从累加器中提取最终的聚合结果。
     *
     * @param acc 累加器实例
     * @return 加权平均值，如果权重和为 0 则返回 null（避免除零异常）
     */
    @Override
    public Double getValue(WeightedAvgAccum acc) {
        if (acc.weightSum == 0) {
            return null;
        }
        return acc.weightedSum / acc.weightSum;
    }
}
