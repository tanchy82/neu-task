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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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

    public ModifyApi(ElasticsearchConfig config){
        esClient = config.getClient();
    }

    @PostMapping("/modify")
    public String modify(@Validated @RequestBody @ApiParam Dto dto){
        modifyData.modify(dto.index, dto.getName(), dto.getData());
        return "success";
    }

    @PostMapping("/modify2")
    public String modify2(){
        Map<String, Set<String>> map = new HashMap<>();
        map.put("test_dev_idx10", null);
        map.put("ihbe_dataset_meta-lichen", null);
        //modifyData2.modifyData2("suzhou_ihbe_dataset_meta-lichen", map);
        return "success";
    }

    @PostMapping("/upload")
    @ResponseBody
    @SneakyThrows
    public String upload(@ApiParam String index, @ApiParam MultipartFile file) throws IOException {
        Assert.notNull(index, "This argument index is required, it must not be null");
        Assert.notNull(index, "This argument file is required, it must not be null");
        ReadExcel.setMap.clear();
        EasyExcel.read(file.getInputStream(), ReadExcel.ExcelData.class, new ReadExcel.ExcelDataListener()).sheet().doRead();
        log.info(ReadExcel.setMap.size() + "");
        ExecutorService executorService = Executors.newFixedThreadPool(16, new ModifyThreadFactory());
        Map<String, Set<String>> task = new ConcurrentHashMap<>();
        while (!ReadExcel.tableQueue.isEmpty()){
            task.putAll(ReadExcel.tableQueue.poll());
            if (task.size() >= 50){
                executorService.execute(new ModifyData2(esClient, index, task));
                task.clear();
            }
        }
        return "success";
    }

    static class ModifyThreadFactory implements ThreadFactory{

        private static final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                    String.format("pool-ModifyData-thread-%s",threadNumber.getAndIncrement()), 0);
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
