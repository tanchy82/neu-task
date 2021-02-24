package com.oldtan.neutask.deleteduplicatedata;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-23
 */
@Slf4j
public class DeleteDuplicateDataThreat implements Runnable {

    private final TransportClient esClient;

    private final String index;

    private final Set<String> rowkeySet;

    public static volatile AtomicLong taskCount = new AtomicLong(0L);

    public static volatile ConcurrentHashMap<String, List<DeleteVo>> deleteCountHashMap = new ConcurrentHashMap<>();

    public DeleteDuplicateDataThreat(TransportClient esClient, String index, Set<String> rowkeySet) {
        this.esClient = esClient;
        this.index = index;
        this.rowkeySet = rowkeySet;
        taskCount.getAndIncrement();
    }

    @Override
    public void run() {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(index);
            searchRequest.source(new SearchSourceBuilder()
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(DuplicateDataAgg.rowkeyFiled, rowkeySet.toArray())))
                    .fetchSource(true).fetchSource(new String[]{DuplicateDataAgg.rowkeyFiled, "CREATE_DATE"}, new String[]{})
                    .size(10000).timeout(new TimeValue(60, TimeUnit.SECONDS)));
            final Map<String, List<DeleteVo>> deleteVoMap = new HashMap<>();
            SearchResponse searchResponse = esClient.search(searchRequest).get();
            Stream.of(searchResponse).filter(Objects::nonNull)
                    .filter((r) -> Objects.equals(r.status(), RestStatus.OK)).forEach(r ->
               StreamSupport.stream(
                       Spliterators.spliteratorUnknownSize(r.getHits().iterator(), Spliterator.ORDERED), false)
                  .map(hit -> DeleteVo.builder().id(hit.getId())
                              .rowkey(String.valueOf(hit.getSourceAsMap().get(DuplicateDataAgg.rowkeyFiled)))
                              .date(Long.parseLong(String.valueOf(hit.getSourceAsMap().get("CREATE_DATE")))).build())
                  .filter((dvo) -> !deleteCountHashMap.containsKey(dvo.rowkey)).forEach(((dvo) ->
                   deleteVoMap.compute(dvo.rowkey, (k, v) -> {
                       v = Objects.isNull(v) ? new ArrayList<>(20) : v ; v.add(dvo);
                       return v; }))));

            deleteVoMap.keySet().stream().forEach((s) -> {
                deleteVoMap.put(s, deleteVoMap.get(s).stream()
                        .sorted(Comparator.comparing(DeleteVo::getDate).reversed()).skip(1).collect(Collectors.toList()));
                deleteCountHashMap.put(s, deleteVoMap.get(s));
            });
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            deleteVoMap.values().stream().forEach((list) -> list.stream()
                    .forEach((vo) -> bulkRequest.add(new DeleteRequest(index, index, vo.id))));
            esClient.bulk(bulkRequest);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            taskCount.getAndDecrement();
        }
    }

    @Data
    @Builder
    static class DeleteVo {
        private String id, rowkey;
        private long date;

    }

}
