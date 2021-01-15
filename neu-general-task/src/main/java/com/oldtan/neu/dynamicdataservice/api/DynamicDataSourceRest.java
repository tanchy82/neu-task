package com.oldtan.neu.dynamicdataservice.api;

import com.oldtan.config.util.PageUtil;
import com.oldtan.neu.dynamicdataservice.constant.Constant;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import com.oldtan.neu.model.entity.DynamicDatasource;
import com.oldtan.neu.model.repository.DynamicDatasourceRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.util.Assert;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotBlank;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-15
 */
@RestController
@RequestMapping("/dynamicDataSource")
@Slf4j
@Api(tags = "Dynamic data source operating restful api.")
public class DynamicDataSourceRest {

    @Autowired
    private SqlDynamicDataSourcePool sqlDynamicDataSourcePool;

    @Autowired
    private DynamicDatasourceRepository dynamicDatasourceRepository;

    private ReentrantLock lock = new ReentrantLock();

    @PostMapping
    @ApiOperation(value = "Create new dynamic data source and return.")
    @SneakyThrows
    public @ApiParam DynamicDatasource create(@Validated @RequestBody @ApiParam final DynamicDatasource datasourceDto){
        /** 1、business logic check */
        Assert.notNull(Constant.SQL_DATABASE_DRIVER_CLASS.get(datasourceDto.getDbType()),
                String.format("This argument is required, data base type is not support.【 %s 】", datasourceDto.toString()));
        Consumer<DynamicDatasource> consumer = (d) -> Assert.state(dynamicDatasourceRepository.findFirstByDbUrlAndDbTypeAndDbUsername(
                    d.getDbUrl(), d.getDbType(), d.getDbUsername()).isPresent(),
                    String.format("This argument is required, link information is exist.【 %s 】", datasourceDto.toString()));
        consumer.accept(datasourceDto);
        /** 2、create DynamicDatasource and store the pool */
        datasourceDto.setId(UUID.randomUUID().toString());
        sqlDynamicDataSourcePool.create(sqlDynamicDataSourcePool.buildSqlDynamicDataSourceVO(datasourceDto));
        /** 3、DynamicDatasource store the data base */
        lock.lock();
        try {
            consumer.accept(datasourceDto);
            datasourceDto.setCreateTime(LocalDateTime.now());
            return dynamicDatasourceRepository.save(datasourceDto);
        }finally {
            lock.unlock();
        }
    }

    @GetMapping("/{id}")
    @ApiOperation("Search a single dynamic data source model by id from path variable parameter.")
    @SneakyThrows
    public @ApiParam DynamicDatasource find(@PathVariable @NotBlank @ApiParam String id){
        return dynamicDatasourceRepository.findById(id).orElseThrow(
                () -> new IllegalStateException(String.format("This argument is required, id %s is not exist.", id)));
    }

    @GetMapping()
    @ApiOperation("Page search dynamic data source.")
    public @ApiParam Page<DynamicDatasource> findAll(@RequestBody @ApiParam cn.hutool.db.Page page){
        return dynamicDatasourceRepository.findAll(PageUtil.toPageRequest(page));
    }

    @DeleteMapping("/{id}")
    @ApiOperation("Delete dynamic data source by id from path variable parameter.")
    public @ApiParam String delete(@PathVariable @NotBlank @ApiParam String id){
        /** 1、check dynamic data source is exist by id  */
        Consumer<String> consumer = (s) -> dynamicDatasourceRepository.findById(s)
                .orElseThrow(() -> new IllegalStateException(String.format("This argument is required, id %s is not exist.", s)));
        /** 1、business logic check */
        consumer = consumer.andThen((s) -> {
            lock.lock();
            try {
                sqlDynamicDataSourcePool.delete(id);
                dynamicDatasourceRepository.deleteById(id);
            }finally {
                lock.unlock();
            }
        });
        consumer.accept(id);
        return String.format("Dynamic data source %s deleted success.",id);
    }

}
