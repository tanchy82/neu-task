package com.oldtan.neu.dynamicdataservice.service.impl;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.Page;
import cn.hutool.db.PageResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import com.oldtan.neu.dynamicdataservice.service.SqlExecute;
import com.oldtan.neu.model.entity.DynamicDataDefinition;
import com.oldtan.neu.model.repository.DynamicDatasourceRepository;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
@Component
public class MysqlSqlExecuteImpl implements SqlExecute {

    @Autowired
    private DynamicDatasourceRepository dynamicDatasourceRepository;

    @Autowired
    private SqlDynamicDataSourcePool sqlDynamicDataSourcePool;

    @Override
    public long getCount(DynamicDataDefinition dataDefinition) {
        return 0;
    }

    @Override
    @SneakyThrows
    public Optional<List<Entity>> getAllTable(SqlDynamicDataSourcePool.SqlDynamicDataSourceVO dataSourceVO) {
        return Optional.ofNullable(Db.use(dataSourceVO.getDataSource()).query(String.format(
                "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s", dataSourceVO.getDbName())));
    }

    @Override
    @SneakyThrows
    public Optional<List<Entity>> getSingleTableDef(SqlDynamicDataSourcePool.SqlDynamicDataSourceVO dataSourceVO, String tableName) {
        return Optional.ofNullable(Db.use(dataSourceVO.getDataSource()).query(String.format(
                "SELECT i.*, t.TABLE_TYPE FROM information_schema.COLUMNS i " +
                        "left join information_schema.TABLES t on t.TABLE_NAME = i.TABLE_NAME " +
                        "WHERE i.TABLE_SCHEMA = %s and i.TABLE_NAME = %s", dataSourceVO.getDbName(), tableName)));
    }

    @Override
    public void insertData(DynamicDataDefinition dataDefinition, JsonNode dataJson) {

    }

    @Override
    public Optional<Entity> getOneRecord(DynamicDataDefinition dataDefinition, String id) {
        return Optional.empty();
    }

    @Override
    public void deleteOneRecord(DynamicDataDefinition dataDefinition, String id) {

    }

    @Override
    public int updateOneRecord(DynamicDataDefinition dataDefinition, String id, JsonNode dataJson) {
        return 0;
    }

    @Override
    public PageResult pageFind(DynamicDataDefinition dataDefinition, JsonNode dataJson, Page page) {
        return null;
    }

    @Override
    public int updateBatch(DynamicDataDefinition dataDefinition, JsonNode dataJson) {
        return 0;
    }

    @Override
    public int insertBatch(DynamicDataDefinition dataDefinition, List<JsonNode> dataJsonList) {
        return 0;
    }
}
