package com.example.flink.client;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 异步数据库客户端
 * 
 * 使用线程池 + CompletableFuture 实现异步查询
 */
public class AsyncDatabaseClient {
    
    private final HikariDataSource dataSource;
    private final ExecutorService executorService;
    
    public AsyncDatabaseClient(String jdbcUrl, String username, String password) {
        // 配置连接池
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        this.dataSource = new HikariDataSource(config);
        
        // 创建线程池用于异步执行
        this.executorService = Executors.newFixedThreadPool(20);
    }
    
    /**
     * 异步查询用户信息
     * 
     * @param userId 用户 ID
     * @return CompletableFuture 包含用户信息的 Map
     */
    public CompletableFuture<Map<String, String>> queryUserAsync(String userId) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                String sql = "SELECT level, region FROM users WHERE user_id = ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, userId);
                
                ResultSet rs = stmt.executeQuery();
                
                Map<String, String> result = new HashMap<>();
                if (rs.next()) {
                    result.put("level", rs.getString("level"));
                    result.put("region", rs.getString("region"));
                } else {
                    // 用户不存在,返回默认值
                    result.put("level", "unknown");
                    result.put("region", "unknown");
                }
                
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Failed to query user: " + userId, e);
            }
        }, executorService);
    }
    
    /**
     * 通用异步查询方法
     * 
     * @param sql SQL 语句
     * @param params 参数
     * @return CompletableFuture 包含查询结果
     */
    public CompletableFuture<Map<String, String>> queryAsync(String sql, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                PreparedStatement stmt = conn.prepareStatement(sql);
                
                // 设置参数
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
                
                ResultSet rs = stmt.executeQuery();
                
                Map<String, String> result = new HashMap<>();
                if (rs.next()) {
                    // 获取所有列
                    int columnCount = rs.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        String value = rs.getString(i);
                        result.put(columnName, value);
                    }
                }
                
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Failed to execute query: " + sql, e);
            }
        }, executorService);
    }
    
    /**
     * 模拟异步查询(用于测试,不需要真实数据库)
     * 
     * @param userId 用户 ID
     * @return CompletableFuture 包含模拟的用户信息
     */
    public CompletableFuture<Map<String, String>> queryUserAsyncMock(String userId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 模拟数据库查询延迟
                Thread.sleep(10);
                
                Map<String, String> result = new HashMap<>();
                // 根据用户 ID 生成模拟数据
                int hash = userId.hashCode();
                result.put("level", hash % 2 == 0 ? "VIP" : "Normal");
                result.put("region", hash % 3 == 0 ? "Beijing" : 
                                    hash % 3 == 1 ? "Shanghai" : "Guangzhou");
                
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Query interrupted", e);
            }
        }, executorService);
    }
    
    /**
     * 关闭客户端,释放资源
     */
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
