package com.metoo.metooelk.controller;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

/**
 * aggregation操作解析
 *
 * @author Metoo
 */
@RestController
@RequestMapping("/aggregation")
public class ElasticSaearchAggregationController {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 根据性别进行分组，查询分组之后的平均年龄
     *
     * @return
     */
    @GetMapping("/findGenderGroupAndAgeAvg")
    public String findGenderGroupAndAgeAvg() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //text类型不能用于索引或排序，必须转成keyword类型
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("metoo_gender")
                .field("gender.keyword");
        //avg_age 为子聚合名称，名称可随意
        aggregation.subAggregation(AggregationBuilders.avg("metoo_avg")
                .field("age"));
        // 将聚合条件加入查询过滤器中
        searchSourceBuilder.aggregation(aggregation);
        // 将查询过滤加入request中
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            // 嗲用search方法获取查询响应
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("aggs query defeated");
        }
        // 获取响应中的返回聚合参数列表
        Aggregations aggregations = searchResponse.getAggregations();
        // 以map形式获取聚合参数
        Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
        // 获取返回参数中名字为metoo_gender的响应参数
        Terms byCompanyAggregation = aggregations.get("metoo_gender");
        // 从Terms响应参数中获取分组之后性别为女的桶数据
        Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("女");
        // 从桶中拿到子聚合中metoo_avg的平均年龄,由于我们请求使用的是AggregationBuilders.avg聚合所以取数据也使用Avg聚合
        Avg averageAge = elasticBucket.getAggregations().get("metoo_avg");
        // 取出
        double avg = averageAge.getValue();
        System.out.println("女性平均年龄：" + avg);
        return "aggs query success";
    }


    /**
     * 查询所有人年龄总和
     *
     * @return
     */
    @GetMapping("/findTotalAge")
    public String findTotalAge() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 使用sum聚合，聚合名为metoo_gender，求和字段为age
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("metoo_gender")
                .field("age");
        // 将sum聚合添加到查询器里面
        searchSourceBuilder.aggregation(sumAggregationBuilder);
        // 将查询器加入请求中
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            // 执行search获取es响应
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 从响应中获取Aggregations聚合参数列表
        Aggregations aggregations = searchResponse.getAggregations();
        // 将聚合参数列表变为map
        Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
        // 使用ParsedSum接收聚合参数。他是Aggregation的子类（Aggregation无法直接取出参数值，需要找到对应聚合查询下的参数接收）
        ParsedSum metooGender = aggregations.get("metoo_gender");
        // 取出值
        double value = metooGender.getValue();
        return "success ok";
    }


}
