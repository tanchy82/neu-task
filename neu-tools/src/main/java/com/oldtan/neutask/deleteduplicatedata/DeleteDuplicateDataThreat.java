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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static volatile ConcurrentLinkedQueue<DeleteVo> LOG_QUEUE = new ConcurrentLinkedQueue();

    public DeleteDuplicateDataThreat(TransportClient esClient, String index, Set<String> rowkeySet) {
        this.esClient = esClient;
        this.index = index;
        this.rowkeySet = rowkeySet;
    }

    @Override
    public void run() {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(index);
            searchRequest.source(new SearchSourceBuilder()
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(DuplicateDataAgg.rowkeyFiled, rowkeySet.toArray())))
                    .fetchSource(true).fetchSource(new String[]{DuplicateDataAgg.rowkeyFiled, "CREATE_DATE"}, new String[]{})
                    .size(20).timeout(new TimeValue(60, TimeUnit.SECONDS)));
            final Map<String, List<DeleteVo>> deleteVoMap = new HashMap<>();
            SearchResponse searchResponse = esClient.search(searchRequest).get();
            Stream.of(searchResponse).filter(Objects::nonNull)
                    .filter((r) -> Objects.equals(r.status(), RestStatus.OK)).forEach(r ->
                    r.getHits().iterator().forEachRemaining((hit) -> {
                        Map<String, Object> hitMap = hit.getSourceAsMap();
                        DeleteVo deleteVo = DeleteVo.builder().id(hit.getId())
                                .rowkey(String.valueOf(hitMap.get(DuplicateDataAgg.rowkeyFiled)))
                                .date(Long.parseLong(String.valueOf(hitMap.get("CREATE_DATE")))).build();
                        if (deleteVoMap.containsKey(deleteVo.rowkey)) {
                            deleteVoMap.get(deleteVo.rowkey).add(deleteVo);
                        } else {
                            List<DeleteVo> temp = new ArrayList<>();
                            temp.add(deleteVo);
                            deleteVoMap.put(deleteVo.rowkey, temp);
                        }
                    })
            );
            deleteVoMap.keySet().stream().forEach((s) -> deleteVoMap.put(s, deleteVoMap.get(s).stream()
                    .sorted(Comparator.comparing(DeleteVo::getDate).reversed()).collect(Collectors.toList()))
            );

            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            deleteVoMap.keySet().stream()
                    .flatMap((s) -> deleteVoMap.get(s).stream())
                    .forEach((s) -> {
                        bulkRequest.add(new DeleteRequest(index,index, s.id));
                        LOG_QUEUE.offer(s);
                        log.info(String.format("%s %s %s", s.id, s.rowkey, s.date));});
            /** delete 从小到大排序 */
            esClient.bulk(bulkRequest);
            /** record log */
            //rowkeySet.stream().forEach((s) -> DuplicateDataAgg.SKIP_LIST_SET.remove(s));

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    @Data
    @Builder
    static class DeleteVo {
        private String id, rowkey;
        private long date;

    }

}
