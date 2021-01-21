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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-19
 */
@Slf4j
public class ModifyData2 implements Runnable{

    private TransportClient esClient;

    public static volatile Map<String, StringBuffer> logBuffersMap = new ConcurrentHashMap<>();

    public static volatile boolean isError = false;

    public static volatile CountDownLatch latch;

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
                    final StringBuffer logBuffer = new StringBuffer();
                    JsonNode mappingJsonNode = strToJson(String.valueOf(hitMap.get("mapping")));
                    logBuffer.append(String.format("********Modify elasticsearch data by index %s , name %s ****** \n", index, name));
                    logBuffer.append(String.format("Before: %s \n", hitMap));
                    logBuffer.append(String.format("Excel : %s \n", map.get(name)));
                    Optional.ofNullable(map.get(name)).ifPresent((s) -> logBuffer.append("Difference: "));
                    UpdateRequest updateRequest = new UpdateRequest();
                    updateRequest.timeout(TimeValue.timeValueSeconds(30L));
                    hitMap.put("hbaseonly", CollectionUtil.isEmpty(map.get(name)));
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(mappingJsonNode.fieldNames(), Spliterator.ORDERED), false)
                            .filter((s) -> CollectionUtil.isNotEmpty(map.get(name)))
                            .filter((s) -> !map.get(name).contains(s))
                            .forEach((s) -> { ((ObjectNode)mappingJsonNode.get(s)).put("index", false);
                                logBuffer.append(String.format(" %s ", s)); });
                    Stream.of(jsonToStr(mappingJsonNode))
                            .filter((s) -> !s.equalsIgnoreCase(String.valueOf(hitMap.get("mapping"))))
                            .forEach((s) -> hitMap.put("mapping", s));
                    updateRequest.index(hit.getIndex()).type(hit.getType()).id(hit.getId()).doc(hitMap);
                    updateBulkRequest.add(updateRequest);
                    logBuffer.append(String.format("\nAfter:  %s \n", hitMap));
                    logBuffersMap.put(name, logBuffer);
                });
                /** 2、modify index data */
                esClient.bulk(updateBulkRequest);
            }
        } catch (Throwable e) {
            isError = true;
            e.printStackTrace();
        }finally {
            latch.countDown();
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
