package com.example.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Flink Elasticsearch Sink 完整示例
 * 
 * 演示如何配置 ElasticsearchSink，包括：
 * 1. 批量写入参数配置
 * 2. 重试机制配置
 * 3. 动态索引名称
 * 4. 失败处理策略
 */
public class ElasticsearchSinkExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkExample.class);
    
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // 启用 Checkpoint，保证 Exactly-Once
        env.enableCheckpointing(60000); // 每分钟 Checkpoint 一次
        
        // 2. 创建数据源（这里用模拟数据源）
        DataStream<UserBehavior> stream = env.addSource(new UserBehaviorSource());
        
        // 3. 配置 Elasticsearch 连接
        List<HttpHost> httpHosts = Arrays.asList(
            new HttpHost("localhost", 9200, "http")
            // 生产环境建议配置多个节点
            // new HttpHost("es-node2", 9200, "http"),
            // new HttpHost("es-node3", 9200, "http")
        );
        
        // 4. 创建 ElasticsearchSink
        ElasticsearchSink.Builder<UserBehavior> builder = new ElasticsearchSink.Builder<>(
            httpHosts,
            new ElasticsearchSinkFunction<UserBehavior>() {
                private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
                
                @Override
                public void process(UserBehavior element, RuntimeContext ctx, RequestIndexer indexer) {
                    // 动态索引名称：按天分索引
                    String indexName = "user_behavior_" + 
                        dateFormat.format(new Date(element.getTimestamp()));
                    
                    // 构建文档内容
                    Map<String, Object> json = new HashMap<>();
                    json.put("user_id", element.getUserId());
                    json.put("action", element.getAction());
                    json.put("product_id", element.getProductId());
                    json.put("category", element.getCategory());
                    json.put("timestamp", element.getTimestamp());
                    
                    // 创建索引请求
                    // 使用唯一 ID 实现幂等写入
                    String docId = element.getUserId() + "_" + element.getTimestamp();
                    
                    IndexRequest request = Requests.indexRequest()
                        .index(indexName)
                        .id(docId)
                        .source(json, XContentType.JSON);
                    
                    indexer.add(request);
                }
            }
        );
        
        // 5. 配置批量写入参数
        // 满足以下任一条件即触发批量写入：
        builder.setBulkFlushMaxActions(1000);      // 每 1000 条数据
        builder.setBulkFlushMaxSizeMb(5);          // 每 5MB 数据
        builder.setBulkFlushInterval(10000);       // 每 10 秒
        
        // 6. 配置重试策略
        // 使用指数退避重试：第 1 次等 1 秒，第 2 次等 2 秒，第 3 次等 4 秒...
        builder.setBulkFlushBackoff(true);
        builder.setBulkFlushBackoffType(
            org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL
        );
        builder.setBulkFlushBackoffRetries(5);     // 最多重试 5 次
        builder.setBulkFlushBackoffDelay(1000);    // 初始延迟 1 秒
        
        // 7. 配置失败处理策略
        builder.setFailureHandler(new CustomFailureHandler());
        
        // 8. 添加 Sink
        stream.addSink(builder.build()).name("Elasticsearch Sink");
        
        // 9. 执行作业
        env.execute("Flink Elasticsearch Sink Example");
    }
    
    /**
     * 自定义失败处理器
     * 
     * 根据不同的错误类型采取不同的处理策略：
     * - 临时性错误（429、超时）：重新入队
     * - 永久性错误（格式错误、索引不存在）：记录日志
     */
    static class CustomFailureHandler implements ActionRequestFailureHandler {
        
        @Override
        public void onFailure(ActionRequest action, Throwable failure,
                              int restStatusCode, RequestIndexer indexer) {
            
            if (restStatusCode == 429) {
                // ES 集群繁忙（Too Many Requests），重新入队
                LOG.warn("ES is busy (429), retrying request: {}", action);
                indexer.add(action);
            } else if (ExceptionUtils.findThrowable(failure, 
                       java.net.SocketTimeoutException.class).isPresent()) {
                // 网络超时，重新入队
                LOG.warn("Network timeout, retrying request: {}", action);
                indexer.add(action);
            } else {
                // 其他错误（格式错误、索引不存在等），记录日志
                // 生产环境建议写入死信队列或告警
                LOG.error("Failed to index document, status: {}, action: {}", 
                         restStatusCode, action, failure);
            }
        }
    }
}
