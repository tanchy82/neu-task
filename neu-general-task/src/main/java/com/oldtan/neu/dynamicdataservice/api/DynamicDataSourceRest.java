package com.oldtan.neu.dynamicdataservice.api;

import com.oldtan.config.util.PageUtil;
import com.oldtan.neu.dynamicdataservice.constant.Constant;
import com.oldtan.neu.dynamicdataservice.model.SqlDataSourceModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import com.oldtan.neu.model.entity.DynamicDatasource;
import com.oldtan.neu.model.repository.DynamicDatasourceRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotBlank;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-15
 */
@RestController
@RequestMapping("/dynamicDataSource")
@Slf4j
@Api(tags = "Dynamic data source operating api.")
public class DynamicDataSourceRest {

    @Autowired
    private SqlDynamicDataSourcePool sqlDynamicDataSourcePool;

    @Autowired
    private DynamicDatasourceRepository dynamicDatasourceRepository;

    private ReentrantLock lock = new ReentrantLock();

    @PostMapping
    @ApiOperation(value = "Create new data source and  return data source model json values. But if exist used exist data source.")
    @SneakyThrows
    public @ApiParam DynamicDatasource create(@Validated @RequestBody @ApiParam DynamicDatasource datasourceDto){
        /** 1、business logic check */
        Stream.of(datasourceDto.getDbType())
                .filter((s) -> Constant.SQL_DATABASE_DRIVER_CLASS.get(s) != null).findAny()
                .orElseThrow(() -> new RuntimeException(
                        String.format("The data base type is not support.【 %s 】", datasourceDto.toString())));
        Consumer<DynamicDatasource> checkIdConsumer = (dto) ->
                dynamicDatasourceRepository.findFirstByDbUrlAndDbTypeAndDbUsername(
                        datasourceDto.getDbUrl(), datasourceDto.getDbType(), datasourceDto.getDbUsername())
                .ifPresent((s1) -> { throw new RuntimeException(
                              String.format("The data source url and username id is exist.【 %s 】", datasourceDto.toString()));});
        checkIdConsumer.accept(datasourceDto);
        /** 2、 TODO must handler different type data base */
        SqlDataSourceModel sqlDataSourceModel = sqlDynamicDataSourcePool.changeVo(datasourceDto);

        DynamicDatasource datasource = new DynamicDatasource();
        BeanUtils.copyProperties(datasourceDto, datasource);
        lock.lock();
        try {
            checkIdConsumer.accept(datasourceDto);
            sqlDataSourceModel = sqlDynamicDataSourcePool.create(sqlDataSourceModel);
            if (sqlDataSourceModel.getId().equalsIgnoreCase(datasourceDto.getId())){
                datasource.setCreateTime(LocalDateTime.now());
                dynamicDatasourceRepository.save(datasource);
            }
            datasourceDto.setId(sqlDataSourceModel.getId());
        }finally {
            lock.unlock();
        }
        return datasourceDto;
    }

    @GetMapping("/{id}")
    @ApiOperation("Search a single data source model by id from path variable parameter.")
    @SneakyThrows
    public @ApiParam DynamicDatasource find(@PathVariable @NotBlank @ApiParam String id){
        return dynamicDatasourceRepository.findById(id)
                .orElseThrow(() -> new RuntimeException(String.format("id=%s data source is not exist.", id)));
    }

    @GetMapping()
    @ApiOperation("Page Search data source model.")
    public @ApiParam Page<DynamicDatasource> findAll(@RequestBody @ApiParam cn.hutool.db.Page page){
        return dynamicDatasourceRepository.findAll(PageUtil.toPageRequest(page));
    }

    @DeleteMapping("/{id}")
    @ApiOperation("Delete data source model by id from path variable parameter.")
    public @ApiParam String delete(@PathVariable @NotBlank @ApiParam String id){
        Consumer<String> consumer = (s) -> dynamicDatasourceRepository.findById(s)
                .orElseThrow(() -> new RuntimeException(String.format("id=%s dataSource is not exist.", s)));
        consumer = consumer.andThen((s) -> {
            lock.lock();
            try {
                /** TODO must handler different type data base */
                sqlDynamicDataSourcePool.delete(id);
                dynamicDatasourceRepository.deleteById(id);
            }finally {
                lock.unlock();
            }
        });
        consumer.accept(id);
        return String.format("id=%s DataSource deleted is success.",id);
    }

}
