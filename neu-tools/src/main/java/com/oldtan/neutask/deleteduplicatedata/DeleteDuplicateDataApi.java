package com.oldtan.neutask.deleteduplicatedata;

import com.oldtan.config.ElasticsearchConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.util.Assert;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-22
 */
@RestController
@Api("Delete elasticsearch Duplicate data Restful API")
@Slf4j
public class DeleteDuplicateDataApi {

    private TransportClient esClient;

    private ExecutorService duplicateDataAggExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Scroll-%s"));

    private ExecutorService findDuplicateDataExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Find-%s"));

    private ExecutorService deleteDuplicateDataExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Delete-%s"));

    private volatile boolean noTaskExecuting = true;

    public DeleteDuplicateDataApi(ElasticsearchConfig config) {
        esClient = config.getClient();
    }

    @PostMapping("/test")
    @SneakyThrows
    public String test() {
        FindDuplicateData.latch = new CountDownLatch(1);
        Set<String> set = new HashSet<>();
        set.add("1a");
        set.add("2a");
        new Thread(new FindDuplicateData(esClient, "tcy01", set)).start();
        FindDuplicateData.latch.await();
        FindDuplicateData.delQueue.stream().forEach(s -> log.info(s));
        return "ok";
    }

    @PostMapping("/deleteDuplicate")
    @SneakyThrows
    public String deleteDuplicate(@Validated @RequestBody @ApiParam Dto dto) {
        Assert.isTrue(noTaskExecuting, "There is current task executing.");
        noTaskExecuting = false;
        /** 1、Rest */
        LocalDateTime startTime = LocalDateTime.now();
        ScrollQuery.RowkeySet.clear();
        FindDuplicateData.delQueue.clear();

        /** 2、Scroll query time range data */
        ScrollQuery.latch = new CountDownLatch(dto.end - dto.start + 1);
        IntStream.rangeClosed(dto.start, dto.end).forEach(
                (i) -> duplicateDataAggExecutorService.execute(new ScrollQuery(esClient, dto.index, i)));
        ScrollQuery.latch.await();
        LocalDateTime scrollQueryTime = LocalDateTime.now();
        long scrollQueryCount = ScrollQuery.RowkeySet.size();
        log.info(String.format("Scroll query rowkey field record %s ", scrollQueryCount));

        /** 3、Find duplicate data */
        if (ScrollQuery.RowkeySet.size() > 0) {
            FindDuplicateData.latch = new CountDownLatch(ScrollQuery.RowkeySet.size() / 1000 + 1);
            while (!ScrollQuery.RowkeySet.isEmpty()) {
                Set<String> temp = ScrollQuery.RowkeySet.stream().limit(1000).collect(Collectors.toSet());
                findDuplicateDataExecutorService.execute(new FindDuplicateData(esClient, dto.index, temp));
                ScrollQuery.RowkeySet.removeAll(temp);
            }
            FindDuplicateData.latch.await();
        }
        LocalDateTime findDuplicateTime = LocalDateTime.now();
        long duplicateDataCount = FindDuplicateData.delQueue.size();
        log.info(String.format("Find duplicate data record %s ", FindDuplicateData.delQueue.size()));

        /** 4、Delete duplicate data */
        if (duplicateDataCount > 0) {
            DeleteDuplicateData.latch = new CountDownLatch(FindDuplicateData.delQueue.size() / 10000 + 1);
            while (!FindDuplicateData.delQueue.isEmpty()) {
                Set<String> idSet = new HashSet<>(10000);
                FindDuplicateData.delQueue.stream().limit(10000).forEach(
                        s -> idSet.add(FindDuplicateData.delQueue.poll().split(" ~#~ ")[0]));
                deleteDuplicateDataExecutorService.execute(new DeleteDuplicateData(esClient, dto.index, idSet));
            }
            DeleteDuplicateData.latch.await();
        }
        LocalDateTime finishTime = LocalDateTime.now();
        noTaskExecuting = true;

        /** 5、Report */
        Supplier<StringBuffer> summaryReport = () -> {
            StringBuffer report = new StringBuffer();
            report.append("\nTask execute report");
            report.append(String.format("\n---Deal with rowkey records: %s", scrollQueryCount));
            report.append(String.format("\n---Delete data records: %s", duplicateDataCount));
            report.append(String.format("\n---Start time: %s", startTime.toString()));
            report.append(String.format("\n---Finish time: %s", finishTime.toString()));
            report.append(String.format("\n---Scroll query time consuming(Millisecond): %s", Duration.between(startTime, scrollQueryTime).toMillis()));
            report.append(String.format("\n---Find duplicate data time consuming(Millisecond): %s", Duration.between(scrollQueryTime, findDuplicateTime).toMillis()));
            report.append(String.format("\n---Delete duplicate data time consuming(Millisecond): %s", Duration.between(findDuplicateTime, finishTime).toMillis()));
            report.append(String.format("\n---Count time consuming(Millisecond): %s", Duration.between(startTime, finishTime).toMillis()));
            return report;
        };
        return summaryReport.get().toString();
    }

    @Data
    @ApiModel("Delete elasticsearch Duplicate data Dto model")
    static class Dto {

        @NotBlank
        @ApiModelProperty("index")
        private String index;

        @NotNull
        @Min(20200101)
        @Max(20211231)
        private Integer start;

        @Min(20200101)
        @Max(20211231)
        private Integer end;
    }

    static class DuplicateDataThreadFactory implements ThreadFactory {

        private static final AtomicInteger threadNumber = new AtomicInteger(1);

        private final String name;

        public DuplicateDataThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                    String.format(name, threadNumber.getAndIncrement()), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

}
