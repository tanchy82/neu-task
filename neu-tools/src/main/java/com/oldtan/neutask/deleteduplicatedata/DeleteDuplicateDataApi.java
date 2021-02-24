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

    private ExecutorService duplicateDataAggExecutorService =
            Executors.newFixedThreadPool(4, new DuplicateDataThreadFactory("Agg-%s"));

    private ExecutorService findDuplicateDataExecutorService =
            Executors.newFixedThreadPool(8, new DuplicateDataThreadFactory("Find-%s"));

    public DeleteDuplicateDataApi(ElasticsearchConfig config) {
        esClient = config.getClient();
    }

    @PostMapping("/deleteDuplicate")
    @SneakyThrows
    public String deleteDuplicate(@Validated @RequestBody @ApiParam Dto dto) {
        LocalDateTime startTime = LocalDateTime.now();
        DeleteDuplicateDataThreat.deleteCountHashMap.clear();
        DuplicateDataAgg.RowkeySet.clear();
        DuplicateDataAgg.isFinish = false;
        DuplicateDataAgg.latch = new CountDownLatch(dto.end - dto.start + 1);
        do {
            duplicateDataAggExecutorService.execute(
                    new DuplicateDataAgg(esClient, dto.index, dto.start, findDuplicateDataExecutorService));
            dto.start = dto.start + 1;
        }while (dto.start <= dto.end);
        DuplicateDataAgg.latch.await();
        LocalDateTime finishTime = LocalDateTime.now();

        Supplier<StringBuffer> summaryReport = () -> {
            StringBuffer report = new StringBuffer();
            report.append("\nTask execute report");
            report.append(String.format("\n---Deal with rowkey records: %s", DuplicateDataAgg.RowkeySet.size()));
            report.append(String.format("\n---Delete data records: %s",
                    DeleteDuplicateDataThreat.deleteCountHashMap.values().stream().mapToInt((m) -> m.size()).sum()));
            report.append(String.format("\n---Task execute start time: %s", startTime.toString()));
            report.append(String.format("\n---Task execute finish time: %s", finishTime.toString()));
            report.append(String.format("\n---Total time consuming(Millisecond): %s", Duration.between(startTime, finishTime).toMillis()));
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
