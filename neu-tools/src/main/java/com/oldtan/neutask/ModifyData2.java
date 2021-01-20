package com.oldtan.neutask;

import cn.hutool.core.collection.CollectionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-19
 */
@Slf4j
public class ModifyData2 implements Runnable{

    private TransportClient esClient;

    public ModifyData2(TransportClient esClient, String index, Map<String, Set<String>> map){
        this.esClient = esClient;
        this.index = index;
        this.map = map;
    }

    private ObjectMapper objectMapper = new ObjectMapper();

    private String index;

    private Map<String, Set<String>> map;

    @Override
    public void run(){
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("enable", true))
                .must(QueryBuilders.termsQuery("name", map.keySet().toArray(new String[map.size()]))));
        sourceBuilder.from(0);
        sourceBuilder.size(map.size());
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.fetchSource(true);
        sourceBuilder.fetchSource(new String[]{"mapping", "name"}, new String[]{});
        searchRequest.source(sourceBuilder);
        try {
            /** 1、Search index data */
            SearchResponse searchResponse = esClient.search(searchRequest).get();
            if (searchResponse.status() == RestStatus.OK){
                BulkRequest updateBulkRequest = new BulkRequest();
                updateBulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                SearchHits hits = searchResponse.getHits();
                hits.iterator().forEachRemaining((hit) -> {
                    Map<String, Object> hitMap = hit.getSourceAsMap();
                    String name = String.valueOf(hitMap.get("name"));
                    JsonNode mappingJsonNode = strToJson(String.valueOf(hitMap.get("mapping")));
                    log.info(String.format("********Modify elasticsearch data by index %s , name %s ******", index, name));
                    log.info(String.format("Before %s", jsonToStr(mappingJsonNode)));
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(mappingJsonNode.fieldNames(), Spliterator.ORDERED), false)
                            .filter((s) -> CollectionUtil.isNotEmpty(map.get(name))
                                    && !map.get(name).contains(s)).forEach((s) -> {
                        ((ObjectNode)mappingJsonNode.get(s)).put("index", false);
                        UpdateRequest updateRequest = new UpdateRequest();
                        updateRequest.timeout(TimeValue.timeValueSeconds(30L));
                        hitMap.put("mapping", jsonToStr(mappingJsonNode));
                        updateRequest.index(hit.getIndex()).type(hit.getType()).id(hit.getId()).doc(hitMap);
                        updateBulkRequest.add(updateRequest); });
                    log.info(String.format("After %s", jsonToStr(mappingJsonNode)));
                });
                /** 2、modify index data */
                esClient.bulk(updateBulkRequest);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private String jsonToStr(JsonNode jsonNode){
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode strToJson(String str){
        try {
            return objectMapper.readTree(str);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


}
