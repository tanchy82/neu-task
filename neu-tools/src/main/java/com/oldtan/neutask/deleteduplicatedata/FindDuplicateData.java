package com.oldtan.neutask.deleteduplicatedata;

import cn.hutool.core.collection.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-23
 */
@Slf4j
public class FindDuplicateData implements Runnable {

    private final TransportClient esClient;

    private final String index;

    private final Set<String> rowkeySet;

    public static volatile CountDownLatch latch;

    public static volatile LinkedBlockingQueue<String> delQueue = new LinkedBlockingQueue();

    public FindDuplicateData(TransportClient esClient, String index, Set<String> rowkeySet) {
        this.esClient = esClient;
        this.index = index;
        this.rowkeySet = rowkeySet;
    }

    @Override
    public void run() {
        try {
            MultiSearchRequestBuilder multiSearch = esClient.prepareMultiSearch();
            rowkeySet.stream().map(s ->
                    new SearchRequest(index).types(index).source(new SearchSourceBuilder()
                            .query(QueryBuilders.boolQuery()
                                    .must(QueryBuilders.existsQuery("CREATE_DATE"))
                                    .filter(QueryBuilders.termsQuery(ScrollQuery.rowkeyFiled, s)))
                            .aggregation(AggregationBuilders.terms("Agg").field(ScrollQuery.rowkeyFiled + ".keyword").minDocCount(2)
                                    .subAggregation(AggregationBuilders.topHits("Top")
                                            .explain(true).fetchSource(true).size(20).from(1).sort("CREATE_DATE.keyword", SortOrder.DESC)
                                            .fetchSource(new String[]{ScrollQuery.rowkeyFiled, "CREATE_DATE"}, new String[]{})))
                            .fetchSource(false).size(0).timeout(new TimeValue(60, TimeUnit.SECONDS)))
            ).forEach(multiSearch::add);
            Stream.of(multiSearch.get().getResponses())
                    .map(MultiSearchResponse.Item::getResponse)
                    .filter(Objects::nonNull)
                    .filter(searchResponse -> Optional.ofNullable(searchResponse.getAggregations().get("Agg")).isPresent())
                    .map(searchResponse -> (Terms) searchResponse.getAggregations().get("Agg"))
                    .map(Terms::getBuckets)
                    .filter(CollectionUtil::isNotEmpty)
                    .flatMap(buckets -> buckets.stream())
                    .filter(bucket -> Optional.ofNullable(bucket.getAggregations().getAsMap().get("Top")).isPresent())
                    .map(bucket -> (TopHits) bucket.getAggregations().get("Top"))
                    .flatMap(topHits -> Arrays.stream(topHits.getHits().getHits()))
                    /*.map(SearchHit::getId)
                    .forEach(delQueue::offer);*/
                    .filter(hit -> Optional.ofNullable(hit.getSourceAsMap()).isPresent())
                    .filter(hit -> Optional.ofNullable(hit.getSourceAsMap().get("CREATE_DATE")).isPresent())
                    .filter(hit -> Optional.ofNullable(hit.getSourceAsMap().get(ScrollQuery.rowkeyFiled)).isPresent())
                    .map(hit -> String.format("%s ~#~ %s ~#~ %s", hit.getId(),
                            hit.getSourceAsMap().get(ScrollQuery.rowkeyFiled), hit.getSourceAsMap().get("CREATE_DATE")))
                    .forEach(delQueue::offer);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }

}
