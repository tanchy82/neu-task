package com.oldtan.neu.dynamicdataservice.api;

import cn.hutool.core.collection.CollectionUtil;
import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataDefinition;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import com.oldtan.neu.dynamicdataservice.service.SqlExecute;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
@RestController
@RequestMapping("/dataDefinition")
public class DataDefinitionRest {

    @Autowired
    private SqlDynamicDataDefinition sqlDynamicDataDefinition;

    @Autowired
    private SqlDynamicDataSourcePool sqlDynamicDataSourcePool;

    @Autowired
    private SqlExecute sqlExecute;

    /**
     * Use data definition id get one data definition service,
     *  and use data source id get more data definition service.
     * @return
     */
    @GetMapping("/{id}")
    public Mono<Object> find(@PathVariable @NotBlank String id){
        Optional<SqlDataDefinitionModel> optional = sqlDynamicDataDefinition.get(id);
        if (optional.isPresent()){
            return Mono.just(ResponseEntity.ok().body(optional.get()));
        }
        List<SqlDataDefinitionModel> definitionModels = sqlDynamicDataDefinition.findBySourceId(id);
        return CollectionUtil.isNotEmpty(definitionModels) ? Mono.just(ResponseEntity.ok().body(definitionModels))
                : Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(String.format("id=%s Data Definition is not exist.", id)));
    }

    /**
     * Create new data definition service.
     * @param model
     * @return
     */
    @PostMapping
    public Mono<Object> create(@Validated @RequestBody SqlDataDefinitionModel model){
        Optional<SqlDataDefinitionModel> optional = sqlDynamicDataDefinition.isExist(model);
        if (optional.isPresent()){
            return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(optional.get()));
        }
        try {
            model.setSqlDataSourceModel(sqlDynamicDataSourcePool.create(model.getSqlDataSourceModel()));
            sqlExecute.getCount(model);
            sqlDynamicDataDefinition.create(model);
            return Mono.just(ResponseEntity.ok().body(model));
        }catch (Exception e){
            return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(String.format("Create data definition failure. Please check table or view object is exist. %s", e.getMessage())));
        }
    }

    @DeleteMapping("/{id}")
    public Mono<Object> delete(@PathVariable @NotBlank String id){
        Optional<SqlDataDefinitionModel> optional = sqlDynamicDataDefinition.get(id);
        if(optional.isPresent()){
            sqlDynamicDataDefinition.delete(optional.get());
            return Mono.just(ResponseEntity.ok().body(String.format("Delete id %s data definition success.", id)));
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Delete id %s data definition failure. Data definition is not exist.", id)));
    }

}
