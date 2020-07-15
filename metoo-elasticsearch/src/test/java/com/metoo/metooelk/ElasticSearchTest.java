package com.metoo.metooelk;

import com.metoo.metooelk.controller.ElasticSaearchAggregationController;
import com.metoo.metooelk.controller.ElasticSearchDocumentController;
import com.metoo.metooelk.controller.ElasticSearchIndexController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ElasticSearchTest {

    @Autowired
    private ElasticSaearchAggregationController elasticSaearchAggregationController;

    @Autowired
    private ElasticSearchDocumentController elasticSearchDocumentController;

    @Autowired
    private ElasticSearchIndexController elasticSearchIndexController;

    @Test
    public  void add(){

    }

}
