package com.oldtan.neu.dynamicdataservice.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataDefinition;
import com.oldtan.neu.dynamicdataservice.service.SqlExecute;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotNull;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-15
 */
@RestController
@RequestMapping("/curd")
@Slf4j
public class CurdRest {

    @Autowired
    private SqlDynamicDataDefinition sqlDynamicDataDefinition;

    @Autowired
    private SqlExecute sqlExecute;

    @GetMapping("/{name}/{id}")
    public Mono<Object> getOne(@PathVariable @NotNull String name, @PathVariable @NotNull String id){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                return Mono.just(ResponseEntity.ok().body(sqlExecute.getOneRecord(definitionModel.get(), id)));
            }catch (Exception e){
                Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s/%s', '%s' variable data definition is not exist.", name,id,name)));
    }

    @GetMapping("/{name}")
    public Mono<Object> getAll(@PathVariable @NotNull String name, @RequestBody @NotNull PageDto page){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                return Mono.just(ResponseEntity.ok().body(
                        sqlExecute.findAll(definitionModel.get(), page.getPageNumber(), page.getPageSize())));
            }catch (Exception e){
                return Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s', '%s' variable data definition is not exist.", name,name)));
    }

    @GetMapping("/{name}/_desc")
    public Mono<Object> def(@PathVariable @NotNull String name){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                return Mono.just(ResponseEntity.ok().body(sqlExecute.getColumnDefinition(definitionModel.get())));
            }catch (Exception e){
                return Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s/_def', '%s' variable data definition is not exist.", name,name)));
    }

    @PostMapping("/{name}")
    public Mono<Object> insert(@PathVariable @NotNull String name, @RequestBody JsonNode jsonNode){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                sqlExecute.insertData(definitionModel.get(), jsonNode);
            }catch (Exception e){
                return Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
            return Mono.just(ResponseEntity.ok().body(jsonNode));
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s', '%s' variable data definition is not exist.", name,name)));
    }

    @DeleteMapping("/{name}/{id}")
    public Mono<Object> delete(@PathVariable @NotNull String name, @PathVariable @NotNull String id){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                sqlExecute.deleteOneRecord(definitionModel.get(), id);
            }catch (Exception e){
                return Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
            return Mono.just(ResponseEntity.ok().body(String.format("Delete id %s data definition is success.", id)));
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s/%s', '%s' variable data definition is not exist.", name,id,name)));
    }

    @PutMapping("/{name}/{id}")
    public Mono<Object> update(@PathVariable @NotNull String name, @PathVariable @NotNull String id, @RequestBody JsonNode jsonNode){
        Optional<SqlDataDefinitionModel> definitionModel = sqlDynamicDataDefinition.get(name);
        if (definitionModel.isPresent()){
            try {
                if(sqlExecute.updateOneRecord(definitionModel.get(), id, jsonNode) > 0) {
                    return Mono.just(ResponseEntity.ok().body(String.format("Update id %s data definition is success.", id)));
                }
            }catch (Exception e){
                return Mono.just(ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(e.getMessage()));
            }
        }
        return Mono.just(ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(String.format("Path variable '/%s/%s', variable data definition or data entity is not exist.", name,id)));
    }

    @Data
    public static class PageDto{
        private int pageNumber=0, pageSize=10;
    }

}
