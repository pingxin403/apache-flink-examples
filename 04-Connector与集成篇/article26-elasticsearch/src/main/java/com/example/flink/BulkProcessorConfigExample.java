package com.example.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

/**
 * BulkProcessor 配置示例
 * 
 * 演示不同场景下的批量写入参数配置：
 * 1. 高吞吐场景
 * 2. 低延迟场景
 * 3. 均衡场景
 */
public class BulkProcessorConfigExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserBehavior> stream = env.addSource(new UserBehaviorSource());
        
        List<HttpHost> httpHosts = Arrays.asList(
            new HttpHost("localhost", 9200, "http")
        );
        
        // 根据不同场景选择配置
        String scenario = "balanced"; // high_throughput, low_latency, balanced
        
        ElasticsearchSink.Builder<UserBehavior> builder = createSinkBuilder(httpHosts, scenario);
        
        stream.addSink(builder.build());
        env.execute("BulkProcessor Config Example - " + scenario);
    }
    
    /**
     * 根据场景创建不同配置的 Sink Builder
     */
    private static ElasticsearchSink.Builder<UserBehavior> createSinkBuilder(
            List<HttpHost> httpHosts, String scenario) {
        
        ElasticsearchSink.Builder<UserBehavior> builder = new ElasticsearchSink.Builder<>(
            httpHosts,
            (element, ctx, indexer) -> {
                Map<String, Object> json = new HashMap<>();
                json.put("user_id", element.getUserId());
                json.put("action", element.getAction());
                json.put("product_id", element.getProductId());
                json.put("timestamp", element.getTimestamp());
                
                IndexRequest request = Requests.indexRequest()
                    .index("user_behavior")
                    .source(json, XContentType.JSON);
                
                indexer.add(request);
            }
        );
        
        switch (scenario) {
            case "high_throughput":
                // 高吞吐场景：适合日志、埋点等大流量场景
                builder.setBulkFlushMaxActions(10000);     // 每 10000 条触发
                builder.setBulkFlushMaxSizeMb(20);         // 每 20MB 触发
                builder.setBulkFlushInterval(30000);       // 每 30 秒触发
                System.out.println("配置：高吞吐场景 - 大批次、长间隔");
                break;
                
            case "low_latency":
                // 低延迟场景：适合实时搜索、监控告警
                builder.setBulkFlushMaxActions(100);       // 每 100 条触发
                builder.setBulkFlushMaxSizeMb(1);          // 每 1MB 触发
                builder.setBulkFlushInterval(1000);        // 每 1 秒触发
                System.out.println("配置：低延迟场景 - 小批次、短间隔");
                break;
                
            case "balanced":
            default:
                // 均衡场景：适合大多数业务场景
                builder.setBulkFlushMaxActions(1000);      // 每 1000 条触发
                builder.setBulkFlushMaxSizeMb(5);          // 每 5MB 触发
                builder.setBulkFlushInterval(10000);       // 每 10 秒触发
                System.out.println("配置：均衡场景 - 中等批次、适中间隔");
                break;
        }
        
        // 通用配置：重试策略
        builder.setBulkFlushBackoff(true);
        builder.setBulkFlushBackoffType(
            org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL
        );
        builder.setBulkFlushBackoffRetries(5);
        builder.setBulkFlushBackoffDelay(1000);
        
        return builder;
    }
}
