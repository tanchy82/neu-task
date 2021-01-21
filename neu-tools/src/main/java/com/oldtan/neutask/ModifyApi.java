package com.oldtan.neutask;

import com.alibaba.excel.EasyExcel;
import com.oldtan.config.ElasticsearchConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-18
 */
@RestController
@Api("Modify elasticsearch data Restful API")
@Slf4j
public class ModifyApi {

    @Autowired
    private ModifyData modifyData;

    private TransportClient esClient;

    ExecutorService executorService = Executors.newFixedThreadPool(16, new ModifyThreadFactory());

    public ModifyApi(ElasticsearchConfig config){
        esClient = config.getClient();
    }

    @PostMapping("/modify")
    public String modify(@Validated @RequestBody @ApiParam Dto dto){
        modifyData.modify(dto.index, dto.getName(), dto.getData());
        return "success";
    }

    @PostMapping("/upload")
    @ResponseBody
    @SneakyThrows
    public String upload(@ApiParam String index, @ApiParam MultipartFile file) throws IOException {
        Assert.notNull(index, "This argument index is required, it must not be null");
        Assert.notNull(index, "This argument file is required, it must not be null");
        LocalDateTime startTime = LocalDateTime.now();
        /** 1、Rest */
        ReadExcel.setMap.clear();
        ModifyData2.isError = false;
        ModifyData2.logBuffersMap.clear();
        /** 2、Analyze Excel */
        EasyExcel.read(file.getInputStream(), ReadExcel.ExcelData.class, new ReadExcel.ExcelDataListener()).sheet("Sheet1").doRead();
        LocalDateTime excelFinishTime = LocalDateTime.now();
        /** 3、Concurrently execute */
        ModifyData2.latch = new CountDownLatch(ReadExcel.tableQueue.size() / 50 + 1);
        Map<String, Set<String>> task = new HashMap<>(50);
        while (!ModifyData2.isError && !ReadExcel.tableQueue.isEmpty()){
            task.putAll(ReadExcel.tableQueue.poll());
            if (task.keySet().size() >= 50 || ReadExcel.tableQueue.isEmpty()){
                executorService.execute(new ModifyData2(esClient, index, task));
                task = new HashMap<>(50);
            }
        }
        ModifyData2.latch.await();
        LocalDateTime finishTime = LocalDateTime.now();
        /** 4、Report */
        Object[] obj = ReadExcel.setMap.keySet().stream()
                .filter((s) -> !ModifyData2.logBuffersMap.keySet().contains(s)).toArray();
        Supplier<StringBuffer> summaryReport = () -> {
            StringBuffer report = new StringBuffer();
            report.append("\nTask execute report");
            report.append(String.format("\n---Result: %s", ModifyData2.isError ? "failure" : "success"));
            report.append(String.format("\n---Task execute time: %s", startTime.toString()));
            report.append(String.format("\n---Total time consuming(Millis): %s", Duration.between(startTime,finishTime).toMillis()));
            report.append(String.format("\n---Analyze Excel time consuming(Millis): %s", Duration.between(startTime,excelFinishTime).toMillis()));
            report.append(String.format("\n---Concurrently execute query and modify elasticsearch data consuming(Millis): %s", Duration.between(excelFinishTime,finishTime).toMillis()));
            report.append(String.format("\n---Need to handle data record: %s", ReadExcel.setMap.size()));
            report.append(String.format("\n---Actual to handle data record: %s", ModifyData2.logBuffersMap.size()));
            report.append(String.format("\n---unprocessed data record: %s", obj.length));
            report.append(String.format("\n---unprocessed data list: %s", Arrays.toString(obj)));
            report.append(String.format("\n---Need to handle data table name list: %s", ReadExcel.setMap.keySet()));
            report.append(String.format("\n---Actual to handle data table name list: %s", ModifyData2.logBuffersMap.keySet()));
            return report;
        };
        Function<StringBuffer, StringBuffer> detailReport = (report) -> {
            report.append("\n---Execution details: \n");
            AtomicInteger i = new AtomicInteger(1);
            ModifyData2.logBuffersMap.keySet().stream()
                    .forEach((key) -> report.append("" + i.getAndIncrement() + ModifyData2.logBuffersMap.get(key)));
            return report;
        };
        StringBuffer summary = summaryReport.get();
        new Thread(() -> log.info(detailReport.apply(summary).toString()),"Modify data report thread").start();
        return summary.toString();
    }

    static class ModifyThreadFactory implements ThreadFactory{

        private static final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                    String.format("pool-ModifyData-%s",threadNumber.getAndIncrement()), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    @Data
    @ApiModel("modify data model")
    static class Dto {
        @NotBlank
        @ApiModelProperty("index")
        private String index;
        @NotBlank
        @ApiModelProperty("index data property name field")
        private String name;
        @NotEmpty
        @ApiModelProperty("index data property mapping field data")
        private Map<String, Map<String, Object>> data;
    }

}
