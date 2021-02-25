package com.oldtan.neutask.deleteduplicatedata;

import cn.hutool.core.collection.CollectionUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-25
 */
public class DeleteService implements Runnable {

    public static volatile AtomicLong delCount = new AtomicLong(0);

    private final TransportClient esClient;

    private final String index;

    public static final String rowkeyFiled = "rowkey";

    public final String rowkeyValue;

    public DeleteService(TransportClient esClient, String index, String rowkeyValue) {
        this.esClient = esClient;
        this.index = index;
        this.rowkeyValue = rowkeyValue;
    }

    @Override
    public void run() {
        try {
            BulkRequest request = new BulkRequest().timeout(TimeValue.timeValueMinutes(2));
            Stream.of(esClient.search(
                    new SearchRequest(index).types(index).source(new SearchSourceBuilder()
                            .query(QueryBuilders.boolQuery()
                                    .must(QueryBuilders.existsQuery("CREATE_DATE"))
                                    .filter(QueryBuilders.termsQuery(rowkeyFiled, rowkeyValue)))
                            .aggregation(AggregationBuilders.terms("Agg").field(rowkeyFiled + ".keyword").minDocCount(2)
                                    .subAggregation(AggregationBuilders.topHits("Top")
                                            .explain(true).fetchSource(true).size(20).from(1).sort("CREATE_DATE.keyword", SortOrder.DESC)
                                            .fetchSource(new String[]{rowkeyFiled, "CREATE_DATE"}, new String[]{})))
                            .fetchSource(false).size(0).timeout(new TimeValue(60, TimeUnit.SECONDS))))
                    .get()).filter(Objects::nonNull)
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
                    .forEach(integer -> {delCount.getAndAdd(integer);esClient.bulk(request);});
            System.gc();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ScrollService.delRowKey.getAndDecrement();
        }
    }
}
