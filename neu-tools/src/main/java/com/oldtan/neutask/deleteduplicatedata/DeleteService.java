package com.oldtan.neutask.deleteduplicatedata;

import cn.hutool.core.collection.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-25
 */
@Slf4j
public class DeleteService implements Runnable {

    public static volatile AtomicLong delCount = new AtomicLong(0);

    private final TransportClient esClient;

    private final String index;

    public static final String rowkeyFiled = "rowkey";

    public final Set<String> rowkeySet;

    public static volatile AtomicLong rowkeyCount = new AtomicLong(0L);

    public DeleteService(TransportClient esClient, String index, Set<String> rowkeySet) {
        this.esClient = esClient;
        this.index = index;
        this.rowkeySet = rowkeySet;
        ScrollService.delRowKey.getAndIncrement();
    }

    @Override
    public void run() {
        try {
            BulkRequest request = new BulkRequest().timeout(TimeValue.timeValueMinutes(2));
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            rowkeySet.stream().map(s ->
                    new SearchRequest(index).types(index).source(new SearchSourceBuilder()
                            .query(QueryBuilders.boolQuery()
                                    .must(QueryBuilders.existsQuery("CREATE_DATE"))
                                    .filter(QueryBuilders.termsQuery(rowkeyFiled, s)))
                            .aggregation(AggregationBuilders.terms("Agg").field(rowkeyFiled + ".keyword").minDocCount(2)
                                    .subAggregation(AggregationBuilders.topHits("Top")
                                            .explain(true).fetchSource(true).size(20).from(1).sort("CREATE_DATE.keyword", SortOrder.DESC)
                                            .fetchSource(new String[]{rowkeyFiled, "CREATE_DATE"}, new String[]{})))
                            .fetchSource(false).size(0).timeout(new TimeValue(60, TimeUnit.SECONDS))))
                    .forEach(multiSearchRequest::add);
            Stream.of(esClient.multiSearch(multiSearchRequest)
                    .get().getResponses()).filter(Objects::nonNull).map(MultiSearchResponse.Item::getResponse)
                    .filter(searchResponse -> Optional.ofNullable(searchResponse.getAggregations().get("Agg")).isPresent())
                    .map(searchResponse -> (Terms) searchResponse.getAggregations().get("Agg"))
                    .map(Terms::getBuckets)
                    .filter(CollectionUtil::isNotEmpty)
                    .flatMap(buckets -> buckets.stream())
                    .filter(bucket -> Optional.ofNullable(bucket.getAggregations().getAsMap().get("Top")).isPresent())
                    .map(bucket -> (TopHits) bucket.getAggregations().get("Top"))
                    .flatMap(topHits -> Arrays.stream(topHits.getHits().getHits()))
                    .map(SearchHit::getId)
                    .forEach(id ->  request.add(new DeleteRequest(index,index,id)));
            Stream.of(request).map(BulkRequest::numberOfActions).filter(integer -> integer > 0)
                    .forEach(integer -> { delCount.getAndAdd(integer);esClient.bulk(request);});
            rowkeyCount.getAndAdd(rowkeySet.size());
            log.info(String.format("Scroll Query rowkey count %s, delete Data: delete %s , Delete count %s",
                    ScrollService.RowkeySet.size(),request.numberOfActions(), delCount.get()));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ScrollService.delRowKey.getAndDecrement();
        }
    }
}
