package com.oldtan.neutask.deleteduplicatedata;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-22
 */
@Slf4j
public class ScrollQuery implements Runnable {

    private final TransportClient esClient;

    public static final String rowkeyFiled = "rowkey";

    private final String index;

    private final int createDate;

    public static volatile ConcurrentSkipListSet<String> RowkeySet = new ConcurrentSkipListSet();

    public static volatile CountDownLatch latch;

    public ScrollQuery(TransportClient esClient, String index, int createDate) {
        this.esClient = esClient;
        this.index = index;
        this.createDate = createDate;
    }

    @Override
    public void run() {
        try {
            SearchResponse scrollResp = esClient.prepareSearch(index)
                    .setScroll(new Scroll(TimeValue.timeValueMinutes(60L)))
                    .setFetchSource(true).setFetchSource(new String[]{ScrollQuery.rowkeyFiled}, new String[]{})
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.wildcardQuery("CREATE_DATE", createDate + "*")))
                    .setSize(10000).get();
            do {
                Stream.of(scrollResp.getHits().getHits())
                        .filter((hit) -> Objects.nonNull(hit.getSourceAsMap()))
                        .map(hit -> hit.getSourceAsMap())
                        .filter(Objects::nonNull)
                        .filter(map -> Objects.nonNull(map.get(ScrollQuery.rowkeyFiled)))
                        .map((map) -> String.valueOf(map.get(ScrollQuery.rowkeyFiled)))
                        .filter(Objects::nonNull)
                        .forEach((s) -> RowkeySet.add(s));
                scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while (scrollResp.getHits().getHits().length != 0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
