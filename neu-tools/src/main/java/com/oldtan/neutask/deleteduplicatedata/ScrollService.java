package com.oldtan.neutask.deleteduplicatedata;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-25
 */
@Slf4j
public class ScrollService implements Runnable {

    private final TransportClient esClient;

    public static final String rowkeyFiled = "rowkey";

    private final String index;

    private final int startDate;

    private final int endDate;

    private ExecutorService deleteExecutorService;

    public static volatile ConcurrentSkipListSet<String> RowkeySet = new ConcurrentSkipListSet();

    public static volatile CountDownLatch latch;

    public static volatile boolean isFinishScroll = false;

    public static volatile AtomicLong delRowKey = new AtomicLong(0);

    public static volatile LinkedBlockingQueue<String> delRowKeyQueue = new LinkedBlockingQueue();

    public ScrollService(TransportClient esClient, String index, int startDate, int endDate, ExecutorService deleteExecutorService) {
        this.esClient = esClient;
        this.index = index;
        this.startDate = startDate;
        this.endDate = endDate;
        isFinishScroll = false;
        this.deleteExecutorService = deleteExecutorService;
    }

    @Override
    public void run() {
        try {
            log.info(String.format("Start scroll query %s ~ %s ... ", startDate, endDate));
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            IntStream.rangeClosed(startDate, endDate).forEach(s ->
                    boolQueryBuilder.should(QueryBuilders.wildcardQuery("CREATE_DATE", s + "*")));
            SearchResponse scrollResp = esClient.prepareSearch(index)
                    .setScroll(new Scroll(TimeValue.timeValueMinutes(60L)))
                    .setFetchSource(true).setFetchSource(new String[]{rowkeyFiled}, new String[]{})
                    .setQuery(boolQueryBuilder).setSize(10000).get();
            Consumer<LinkedBlockingQueue<String>> carry = (q) ->
                    deleteExecutorService.execute(new DeleteService(esClient, index,
                            IntStream.rangeClosed(1, 1000).filter(i -> q.size() > 0).mapToObj(i -> q.poll()).collect(Collectors.toSet())));
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
                            delRowKeyQueue.offer(s);
                            if (delRowKeyQueue.size() > 1000)  carry.accept(delRowKeyQueue);
                        });
                scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while (scrollResp.getHits().getHits().length != 0);
            log.info(String.format("Finish scroll query %s ~ %s ... ", startDate, endDate));
            isFinishScroll = true;
            carry.accept(delRowKeyQueue);
            while (delRowKey.get() > 0) { }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }

}
