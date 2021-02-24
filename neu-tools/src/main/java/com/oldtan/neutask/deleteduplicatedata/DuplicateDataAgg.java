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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
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

    private final String createDate;

    private final ExecutorService executorTask;

    public static volatile boolean isFinish = false;

    public static volatile ConcurrentSkipListSet<String> RowkeySet = new ConcurrentSkipListSet();

    public static volatile CountDownLatch latch;

    public DuplicateDataAgg(TransportClient esClient, String index, int createDate, ExecutorService executorTask) {
        this.esClient = esClient;
        this.index = index;
        this.executorTask = executorTask;
        this.createDate = createDate + "*";
    }

    @Override
    public void run() {
        try {
            SearchResponse scrollResp = esClient.prepareSearch(index)
                    .setScroll(new Scroll(TimeValue.timeValueMinutes(60L)))
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.wildcardQuery("CREATE_DATE", createDate)))
                    .setFetchSource(true).setFetchSource(new String[]{DuplicateDataAgg.rowkeyFiled}, new String[]{})
                    .setSize(100).get();
            do {
                Stream.of(Stream.of(scrollResp.getHits().getHits())
                        .filter((hit) -> Objects.nonNull(hit.getSourceAsMap()))
                        .filter((hit) -> Objects.nonNull(hit.getSourceAsMap().get(DuplicateDataAgg.rowkeyFiled)))
                        .map((hit) -> String.valueOf(hit.getSourceAsMap().get(DuplicateDataAgg.rowkeyFiled)))
                        .filter(((s) -> !RowkeySet.contains(s)))
                        .collect(Collectors.toSet())).filter((set) -> !set.isEmpty())
                        .forEach((set) -> { RowkeySet.addAll(set);
                                executorTask.execute(new DeleteDuplicateDataThreat(esClient, index, set));});
                scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0);

            while (DeleteDuplicateDataThreat.taskCount.get() != 0L) {

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
