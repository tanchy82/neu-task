package com.oldtan.neutask.deleteduplicatedata;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-25
 */
public class ScrollService implements Runnable {

    private final TransportClient esClient;

    public static final String rowkeyFiled = "rowkey";

    private final String index;

    private final int createDate;

    public static volatile ConcurrentSkipListSet<String> RowkeySet = new ConcurrentSkipListSet();

    public static volatile CountDownLatch latch;

    private final ExecutorService deleteExecutorService;

    public static volatile AtomicLong delRowKey = new AtomicLong(0);

    public ScrollService(TransportClient esClient, String index, int createDate, ExecutorService deleteExecutorService) {
        this.esClient = esClient;
        this.index = index;
        this.createDate = createDate;
        this.deleteExecutorService = deleteExecutorService;
    }

    @Override
    public void run() {
        try {
            SearchResponse scrollResp = esClient.prepareSearch(index)
                    .setScroll(new Scroll(TimeValue.timeValueMinutes(60L)))
                    .setFetchSource(true).setFetchSource(new String[]{rowkeyFiled}, new String[]{})
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.wildcardQuery("CREATE_DATE", createDate + "*")))
                    .setSize(10000).get();
            do {
                Stream.of(scrollResp.getHits().getHits())
                        .filter((hit) -> Objects.nonNull(hit.getSourceAsMap()))
                        .map(hit -> hit.getSourceAsMap())
                        .filter(Objects::nonNull)
                        .filter(map -> Objects.nonNull(map.get(rowkeyFiled)))
                        .map((map) -> String.valueOf(map.get(rowkeyFiled)))
                        .filter(Objects::nonNull)
                        .filter(s -> !RowkeySet.contains(s))
                        .forEach((s) -> {
                            RowkeySet.add(s);
                            delRowKey.getAndIncrement();
                            deleteExecutorService.execute(new DeleteService(esClient, index, s));
                        });
                scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while (scrollResp.getHits().getHits().length != 0);
            while (delRowKey.get() > 0){ }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }

}
