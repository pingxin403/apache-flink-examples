package com.example.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 手机号脱敏 UDF（标量函数）
 *
 * <p>功能：将 11 位手机号的中间 4 位替换为 ****
 * <p>示例：13812345678 → 138****5678
 *
 * <p>UDF 是最简单的自定义函数类型，输入一行数据，输出一个值（一对一映射）。
 * 开发步骤：
 * 1. 继承 ScalarFunction
 * 2. 实现 eval 方法（方法名固定，Flink 通过反射调用）
 * 3. 使用 @DataTypeHint 标注参数和返回值类型
 *
 * <p>SQL 使用方式：
 * <pre>
 * CREATE TEMPORARY FUNCTION mask_phone AS 'com.example.flink.MaskPhoneUdf';
 * SELECT user_id, mask_phone(phone) AS masked_phone FROM user_info;
 * </pre>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/udfs/">
 *     Flink UDF 官方文档</a>
 */
public class MaskPhoneUdf extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MaskPhoneUdf.class);

    /** 标准中国大陆手机号长度 */
    private static final int PHONE_LENGTH = 11;

    /** 脱敏掩码 */
    private static final String MASK = "****";

    /**
     * 对手机号进行脱敏处理。
     *
     * <p>规则：
     * - 标准 11 位手机号：保留前 3 位和后 4 位，中间用 **** 替换
     * - null 或非 11 位字符串：原样返回，不做处理
     *
     * @param phone 原始手机号字符串
     * @return 脱敏后的手机号，如 138****5678
     */
    public String eval(@DataTypeHint("STRING") String phone) {
        try {
            if (phone == null || phone.length() != PHONE_LENGTH) {
                return phone;
            }
            // 保留前 3 位 + **** + 后 4 位
            return phone.substring(0, 3) + MASK + phone.substring(7);
        } catch (Exception e) {
            // 捕获异常避免整个作业失败，记录日志后返回原值
            LOG.warn("手机号脱敏处理失败, phone={}", phone, e);
            return phone;
        }
    }

    /**
     * 重载方法：支持指定保留前缀和后缀的长度。
     *
     * <p>示例：eval("13812345678", 4, 3) → 1381***678
     *
     * @param phone      原始手机号
     * @param prefixLen  保留前缀长度
     * @param suffixLen  保留后缀长度
     * @return 脱敏后的手机号
     */
    public String eval(
            @DataTypeHint("STRING") String phone,
            @DataTypeHint("INT") Integer prefixLen,
            @DataTypeHint("INT") Integer suffixLen) {
        try {
            if (phone == null || prefixLen == null || suffixLen == null) {
                return phone;
            }
            if (prefixLen + suffixLen >= phone.length()) {
                return phone;  // 前缀+后缀长度超过总长度，无法脱敏
            }
            int maskLen = phone.length() - prefixLen - suffixLen;
            String mask = "*".repeat(maskLen);
            return phone.substring(0, prefixLen) + mask + phone.substring(phone.length() - suffixLen);
        } catch (Exception e) {
            LOG.warn("手机号脱敏处理失败, phone={}, prefixLen={}, suffixLen={}", phone, prefixLen, suffixLen, e);
            return phone;
        }
    }
}
