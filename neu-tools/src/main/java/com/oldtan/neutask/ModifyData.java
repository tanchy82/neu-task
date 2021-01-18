package com.oldtan.neutask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.oldtan.config.ElasticsearchConfig;
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
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-18
 */
@Slf4j
@Component
public class ModifyData {

    private TransportClient esClient = ElasticsearchConfig.esClient;

    private ObjectMapper objectMapper = new ObjectMapper();

    public void modify(String index, String name, Map<String, String> newMapping){
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("enable", true))
                .must(QueryBuilders.termsQuery("name", name)));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.fetchSource(true);
        sourceBuilder.fetchSource("mapping","");
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest).get();
            /** 1、Search index data */
            if (searchResponse.status() == RestStatus.OK){
                SearchHits hits = searchResponse.getHits();
                BulkRequest bulkRequest = new BulkRequest();
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                hits.iterator().forEachRemaining((hit) -> {
                   Map<String, Object> oldMap = hit.getSourceAsMap();
                   try {
                       JsonNode jsonNode = objectMapper.readTree(String.valueOf(oldMap.get("mapping")));
                       StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(jsonNode.fieldNames(), Spliterator.ORDERED), false)
                                .filter((s) -> newMapping.containsKey(s)).forEach((s) ->
                                ((ObjectNode)jsonNode.get(s)).put("type", newMapping.get(s))
                       );
                       System.out.println(objectMapper.writeValueAsString(jsonNode));
                       oldMap.put("mapping", objectMapper.writeValueAsString(jsonNode));

                       UpdateRequest updateRequest = new UpdateRequest();
                       updateRequest.timeout("30s");
                       updateRequest.index(hit.getIndex()).type(hit.getType()).id(hit.getId()).doc(oldMap);
                       bulkRequest.add(updateRequest);
                   } catch (JsonProcessingException e) {
                        e.printStackTrace();
                   }
                });
                /** 2、modify index data */
                esClient.bulk(bulkRequest);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mm = new HashMap<>(1);
        mm.put("mapping", "boolean");
        String str = "{\"mapping\":{\"type\":\"text\"},\"enable\":{\"type\":\"boolean\"},\"name\":{\"type\":\"keyword\"},\"alias\":{\"type\":\"keyword\"}}";
        try {
            JsonNode jsonNode = objectMapper.readTree(str);
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(jsonNode.fieldNames(), Spliterator.ORDERED), false)
                    .filter((s) -> mm.containsKey(s)).forEach((s) ->
                ((ObjectNode)jsonNode.get(s)).put("type", mm.get(s))
            );
            System.out.println(objectMapper.writeValueAsString(jsonNode));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
