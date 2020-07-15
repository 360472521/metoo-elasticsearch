package com.metoo.metooelk.controller;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * index操作解析
 *
 * @author Metoo
 */
@RestController
@RequestMapping("/index")
public class ElasticSearchIndexController {


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 参数解析：
     * new new CreateIndexRequest("metoo");  // metoo我们创建的索引名称
     * CreateIndexResponse  // 执行完毕之后返回的创建信息，里面包含es的分片副本等
     * RequestOptions.DEFAULT  // es访问方式，我们使用默认访问方式
     */
    @GetMapping("/createIndex")
    public String createIndex() {
        // 创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("metoo");
        try {
            // 执行创建请求
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            // 响应
            System.out.println(createIndexResponse);
            return "test success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("create index defeated");
        }
    }

    @GetMapping("/getIndex")
    public String getIndex() {
        GetIndexRequest getIndexRequest = new GetIndexRequest("metoo");
        try {
            // 获取index对象
            GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
            // 打印索引对象
            System.out.println(getIndexResponse);
            // 检查索引是否存在
            boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            System.out.println("索引是否存在：" + exists);
            return "find index success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("find index defeated");
        }
    }

    @GetMapping("/deleteIndex")
    public String deleteIndex() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("metoo");
        try {
            // 删除索引对象
            AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            // 打印删除索引对象
            System.out.println(delete);
            return "delete index success";
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("delete index defeated");
        }
    }
}
