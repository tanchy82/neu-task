package com.oldtan.neu.dynamicdataservice.api;

import com.oldtan.neu.dynamicdataservice.api.dto.DynamicDatasourceDto;
import com.oldtan.neu.dynamicdataservice.constant.Constant;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSource;
import com.oldtan.neu.model.entity.DynamicDatasource;
import com.oldtan.neu.model.repository.DynamicDatasourceRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotBlank;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

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
    private SqlDynamicDataSource sqlDynamicDataSource;

    @Autowired
    private DynamicDatasourceRepository dynamicDatasourceRepository;

    private ReentrantLock lock = new ReentrantLock();

    @PostMapping
    @ApiOperation(value = "Create new data source and  return data source model json values. But if exist used exist data source.")
    public DynamicDatasource create(@Validated @RequestBody @ApiParam DynamicDatasourceDto datasourceDto){
        Supplier<DynamicDatasource> supplier = () -> {
            DynamicDatasource datasource = new DynamicDatasource();
            sqlDynamicDataSource.get(datasourceDto.getId());
            if (Constant.sqlDatabaseDriverClass.get(datasourceDto.getDbType()) != null){
                datasource.setId(datasourceDto.getId());
                datasource.setDbType(datasourceDto.getDbType());
                datasource.setDbConnect(datasourceDto.getDbConnect().asText());
                lock.lock();
                try {
                    if (Constant.sqlDatabaseDriverClass.get(datasourceDto.getDbType()) != null){
                        sqlDynamicDataSource.create(sqlDynamicDataSource.changeVo(datasourceDto));
                        datasource.setCreateTime(LocalDateTime.now());
                        dynamicDatasourceRepository.save(datasource);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    lock.unlock();
                }
            }
            return datasource;
        };
        Optional<DynamicDatasource> optional =
                Optional.ofNullable(Optional.ofNullable(dynamicDatasourceRepository.getOne(datasourceDto.getId()))
                        .orElseGet(supplier));
        return optional.get();
        /*try {
            return Mono.just(ResponseEntity.ok().body(sqlDynamicDataSource.create(sourceModel)));
        } catch (Exception e) {
            return Mono.just(ResponseEntity.unprocessableEntity()
                    .body(String.format("Datasource initialization failure. %s", e.getMessage())));
        }*/
    }


    @GetMapping("/{id}")
    @ApiOperation("Search data source model by id from path variable parameter.")
    public Mono<Object> find(@PathVariable @NotBlank String id){
        if (sqlDynamicDataSource.isExist(id)){
            return Mono.just(ResponseEntity.ok()
                    .body(sqlDynamicDataSource.get(id)));
        }else {
            return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(String.format("id=%s DataSource is not exist.", id)));
        }
    }

    @GetMapping()
    @ApiOperation("Page Search data source model.")
    public Mono<Object> findAll(){
        //todo page search
        return Mono.just(ResponseEntity.ok().body(sqlDynamicDataSource.findAll()));
    }

    @DeleteMapping("/{id}")
    @ApiOperation("Delete data source model by id from path variable parameter.")
    public Mono<Object> delete(@PathVariable @NotBlank String id){
        if (sqlDynamicDataSource.isExist(id)){
            sqlDynamicDataSource.delete(id);
            return Mono.just(ResponseEntity.ok().body(String.format("id=%s DataSource deleted is success.",id)));
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("id=%s DataSource is not exist.", id)));
    }

}
