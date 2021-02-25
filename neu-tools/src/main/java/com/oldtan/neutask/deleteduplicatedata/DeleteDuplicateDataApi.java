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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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

    private ExecutorService scrollExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Scroll-%s"));

    private ExecutorService deleteExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Delete-%s"));

    private volatile boolean noTaskExecuting = true;

    public DeleteDuplicateDataApi(ElasticsearchConfig config) {
        esClient = config.getClient();
    }

    @PostMapping("/deleteDuplicateData")
    @SneakyThrows
    public String deleteDuplicateData(@Validated @RequestBody @ApiParam Dto dto) {
        Assert.isTrue(noTaskExecuting, "There is current task executing.");
        noTaskExecuting = false;
        /** 1、Rest */
        LocalDateTime startTime = LocalDateTime.now();
        DeleteService.delCount.set(0L);
        ScrollService.RowkeySet.clear();
        ScrollService.delRowKey.set(0L);

        /** 2、Task */
        ScrollService.latch = new CountDownLatch(1);
        scrollExecutorService.execute(new ScrollService(esClient, dto.index, dto.start, deleteExecutorService));
        ScrollService.latch.await();
        LocalDateTime finishTime = LocalDateTime.now();
        noTaskExecuting = true;

        /** 3、Report */
        Supplier<StringBuffer> summaryReport = () -> {
            StringBuffer report = new StringBuffer();
            report.append("\nTask execute report");
            report.append(String.format("\n---Deal with rowkey records: %s", ScrollService.RowkeySet.size()));
            report.append(String.format("\n---Delete data records: %s", DeleteService.delCount.longValue()));
            report.append(String.format("\n---Start time: %s", startTime.toString()));
            report.append(String.format("\n---Finish time: %s", finishTime.toString()));
            report.append(String.format("\n---Count time consuming(Millisecond): %s", Duration.between(startTime, finishTime).toMillis()));
            return report;
        };
        StringBuffer report = summaryReport.get();
        log.info(report.toString());
        return report.toString();
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
