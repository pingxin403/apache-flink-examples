package com.example.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 动态索引示例
 * 
 * 演示如何根据数据内容动态生成索引名称：
 * 1. 按日期分索引（按天）
 * 2. 按类目分索引
 * 3. 按日期+类目分索引
 */
public class DynamicIndexExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserBehavior> stream = env.addSource(new UserBehaviorSource());
        
        List<HttpHost> httpHosts = Arrays.asList(
            new HttpHost("localhost", 9200, "http")
        );
        
        // 选择索引策略
        String strategy = "date_category"; // date, category, date_category
        
        ElasticsearchSink.Builder<UserBehavior> builder = new ElasticsearchSink.Builder<>(
            httpHosts,
            createSinkFunction(strategy)
        );
        
        // 配置批量写入参数
        builder.setBulkFlushMaxActions(1000);
        builder.setBulkFlushMaxSizeMb(5);
        builder.setBulkFlushInterval(10000);
        
        stream.addSink(builder.build());
        env.execute("Dynamic Index Example - " + strategy);
    }
    
    /**
     * 根据策略创建不同的 SinkFunction
     */
    private static ElasticsearchSinkFunction<UserBehavior> createSinkFunction(String strategy) {
        return new ElasticsearchSinkFunction<UserBehavior>() {
            private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
            
            @Override
            public void process(UserBehavior element, RuntimeContext ctx, RequestIndexer indexer) {
                String indexName;
                
                switch (strategy) {
                    case "date":
                        // 策略1：按日期分索引
                        // 索引名称：user_behavior_2025_01_15
                        indexName = "user_behavior_" + 
                            dateFormat.format(new Date(element.getTimestamp()));
                        System.out.println("按日期分索引: " + indexName);
                        break;
                        
                    case "category":
                        // 策略2：按类目分索引
                        // 索引名称：user_behavior_电子产品
                        indexName = "user_behavior_" + 
                            element.getCategory().replaceAll("\\s+", "_");
                        System.out.println("按类目分索引: " + indexName);
                        break;
                        
                    case "date_category":
                    default:
                        // 策略3：按日期+类目分索引
                        // 索引名称：user_behavior_2025_01_15_电子产品
                        indexName = "user_behavior_" + 
                            dateFormat.format(new Date(element.getTimestamp())) + "_" +
                            element.getCategory().replaceAll("\\s+", "_");
                        System.out.println("按日期+类目分索引: " + indexName);
                        break;
                }
                
                // 构建文档
                Map<String, Object> json = new HashMap<>();
                json.put("user_id", element.getUserId());
                json.put("action", element.getAction());
                json.put("product_id", element.getProductId());
                json.put("category", element.getCategory());
                json.put("timestamp", element.getTimestamp());
                
                // 创建索引请求
                IndexRequest request = Requests.indexRequest()
                    .index(indexName)
                    .source(json, XContentType.JSON);
                
                indexer.add(request);
            }
        };
    }
}
