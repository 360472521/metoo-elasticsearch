package com.metoo.metooelk.controller;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * document操作解析
 *
 * @author Metoo
 */
@RestController
@RequestMapping("/document")
public class ElasticSearchDocumentController {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private final String INDEX_NAME = "customer";

    /**
     * 参数解析：
     * IndexRequest //索引请求对象
     * jsonMap  // 这里我们使用Map封装文档参数，然后使用source为我们解析参数
     */
    @GetMapping("/insertDocument")
    public String insertDocument() {
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.id("1");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "张三");
        jsonMap.put("gender", "男");
        jsonMap.put("age", 18);
        jsonMap.put("phone", "133111111111");
        request.source(jsonMap);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            System.out.println(indexResponse);
            return "create document success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("create Document defeated");
        }
    }


    @GetMapping("/findDocumentById")
    public String findDocumentById() {
        // 使用getRequest定义我们需要查询的索引与文档。
        GetRequest getRequest = new GetRequest(INDEX_NAME, "1");
        try {
            //获取查询响应
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(JSONObject.toJSON(getResponse));
            // 从响应中获取响应参数
            Map<String, Object> source = getResponse.getSource();
            System.out.println(source.get("name"));
            return "find document by id success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("find document by id defeated");
        }
    }

    /**
     * 查询name中包含张的数据
     *
     * @return
     */
    @GetMapping("/findDocumentByName")
    public String findDocumentByName() {
        try {
            // 新建查询请求
            SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
            // 使用bool组合查询
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // 封装查询条件 使用matchQuery
            // 查询name包含张的数据
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "张");
            // 查询age不为25岁的数据
            MatchQueryBuilder matchNoteQuery = QueryBuilders.matchQuery("age", 25);
            // 将我们定义的match和matchNot封装进bool
            boolQueryBuilder.must(matchQuery);
            boolQueryBuilder.mustNot(matchNoteQuery);
            // 创建查询器，加入bool过滤器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQueryBuilder);
            // 将查询器加入到request请求当中
            searchRequest.source(searchSourceBuilder);
            // 调用highClient的search方法传入request查询结果集
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            // 取出结果集中的hits
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
            return "bool query success";
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("bool query defeated");
        }
    }

    /**
     * 根据文档id删除文档
     *
     * @return
     */
    @GetMapping("/deleteDocumentById")
    public String deleteDocumentById() {
        try {
            // 使用deleteRequest对象，删除id为3的文档
            DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME, "3");
            // 请求方式为默认
            DeleteResponse deleteResponse = restHighLevelClient.delete(
                    deleteRequest, RequestOptions.DEFAULT);
            System.out.println(deleteResponse);
            return "delete document by id success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("delete document by id defeated");
        }
    }


    /**
     * 修改文档
     *
     * @return
     */
    @GetMapping("/updateDocumentById")
    public String updateDocumentById() {

        try {
            // 使用updateRequest修改文档id为1的文档
            UpdateRequest request = new UpdateRequest(INDEX_NAME, "1");
            // 修改我们使用json字符串的方式
            String jsonString = "{" +
                    "\"age\":\"30\"" +
                    "}";
            // 标识参数为json字符串
            request.doc(jsonString, XContentType.JSON);
            // 请求方式为RequestOptions.DEFAULT默认
            UpdateResponse updateResponse = restHighLevelClient.update(
                    request, RequestOptions.DEFAULT);
            System.out.println(updateResponse);
            return "update document by id success";
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("update document by id defeated");
        }
    }

    /**
     * 聚合查询,用户平均年龄
     *
     * @return
     */
    @GetMapping("/findAggrUserAge")
    public String findAggrUserAge() {

        try {
            SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            AvgAggregationBuilder age = AggregationBuilders.avg("age");
            searchSourceBuilder.aggregation(age);
            searchRequest.source(searchSourceBuilder);
            SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(search);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "success ok";
    }
}
