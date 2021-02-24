package com.oldtan.neutask.deleteduplicatedata;

import cn.hutool.core.collection.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-22
 */
@Slf4j
public class DuplicateDataAgg implements Runnable {

    private final TransportClient esClient;

    public static final String rowkeyFiled = "rowkey";

    private final String index;

    private final ExecutorService executorTask;

    private static volatile boolean isFinish = false;

    public static volatile ConcurrentSkipListSet<String> RowkeySet = new ConcurrentSkipListSet();

    public static volatile CountDownLatch latch;

    public DuplicateDataAgg(TransportClient esClient, String index, ExecutorService executorTask) {
        this.esClient = esClient;
        this.index = index;
        this.executorTask = executorTask;
    }

    @Override
    public void run() {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(index);
        try {
            while (!isFinish) {
                searchRequest.source(new SearchSourceBuilder()
                        .aggregation(AggregationBuilders.terms("groupAgg").field(rowkeyFiled + ".keyword").minDocCount(2).size(100).executionHint("map"))
                        .size(0).timeout(new TimeValue(600, TimeUnit.SECONDS)));
                Set<String> rowkeySet = new HashSet<>(100);
                SearchResponse searchResponse = esClient.search(searchRequest).get();

                Stream.of(searchResponse).filter(Objects::nonNull)
                        .filter((r) -> Objects.equals(r.status(), RestStatus.OK)).forEach(r -> {
                    StringTerms terms = (StringTerms) r.getAggregations().getAsMap().get("groupAgg");
                    if (terms.getBuckets().isEmpty()) isFinish = true; else
                    terms.getBuckets().stream().filter((bucket) -> !RowkeySet.contains(bucket.getKeyAsString()))
                            .forEach((bucket) -> {
                                RowkeySet.add(bucket.getKeyAsString());
                                rowkeySet.add(bucket.getKeyAsString());
                            });
                });
                Stream.of(rowkeySet).filter(CollectionUtil::isNotEmpty).forEach((set) ->
                        executorTask.execute(new DeleteDuplicateDataThreat(esClient, index, rowkeySet)));
            }
            while (DeleteDuplicateDataThreat.taskCount.get() != 0L) {

            }
            latch.countDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}