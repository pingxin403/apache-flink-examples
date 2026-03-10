-- 创建数据库
CREATE DATABASE IF NOT EXISTS flink_demo;
USE flink_demo;

-- 创建订单表
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY COMMENT '订单ID',
    user_id VARCHAR(64) NOT NULL COMMENT '用户ID',
    amount DECIMAL(10, 2) NOT NULL COMMENT '订单金额',
    create_time DATETIME NOT NULL COMMENT '创建时间',
    INDEX idx_user_id (user_id),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表';

-- 查看表结构
DESC orders;

-- 查询示例
-- SELECT * FROM orders ORDER BY create_time DESC LIMIT 10;
-- SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount 
-- FROM orders GROUP BY user_id ORDER BY total_amount DESC LIMIT 10;
