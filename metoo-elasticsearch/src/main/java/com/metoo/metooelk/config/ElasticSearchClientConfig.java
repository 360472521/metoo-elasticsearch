package com.metoo.metooelk.config;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// 配置注解
@Configuration
public class ElasticSearchClientConfig {
    //注入es高级客户端
    @Bean
    public RestHighLevelClient restHighLevelClient(){
        // 定义连接地址,如果是多个就配置多个HttpHost，由于我们使用的是RestApi进行访问所以使用的是Http方式9200（使用Java Api则是9300端口）
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("112.74.48.31", 9200, "http"));

        // 一些其他连接配置，例如超时时间等
        builder.setRequestConfigCallback(
                new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(
                            RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setSocketTimeout(10000);
                    }
                });

        // 异步调用配置，配置监听器等
        // 由于未设置监听器，所以不开放该配置
        /*builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(
                    HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setProxy(
                        new HttpHost("112.74.48.31", 8080, "http"));
            }
        });*/
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
