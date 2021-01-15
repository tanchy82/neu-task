package com.oldtan.neu.dynamicdataservice.service;

import cn.hutool.db.Entity;
import cn.hutool.db.Page;
import cn.hutool.db.PageResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.oldtan.neu.model.entity.DynamicDataDefinition;

import java.util.List;
import java.util.Optional;

/**
 * @Description: Sql execute interface
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
public interface SqlExecute {

    /**
     * Query dynamic data definition object record count.
     * @param dataDefinition
     * @return
     */
    long getCount(DynamicDataDefinition dataDefinition);

    /**
     * Query dynamic data source object all table definition.
     * @param datasource
     * @return
     */
    Optional<List<Entity>> getAllTable(SqlDynamicDataSourcePool.SqlDynamicDataSourceVO datasource);

    /**
     * Query single dynamic data definition table definition.
     * @param datasource
     * @param tableName
     * @return
     */
    Optional<List<Entity>> getSingleTableDef(SqlDynamicDataSourcePool.SqlDynamicDataSourceVO datasource, String tableName);

    /**
     * Insert dynamic data definition object one record by json.
     * @param dataDefinition
     * @param dataJson
     * @return
     */
    void insertData(DynamicDataDefinition dataDefinition, JsonNode dataJson);

    /**
     * Query dynamic data definition object one record by id.
     * @param dataDefinition
     * @param id
     * @return
     */
    Optional<Entity> getOneRecord(DynamicDataDefinition dataDefinition, String id);

    /**
     * Delete dynamic data definition object one record by id.
     * @param dataDefinition
     * @param id
     * @return
     */
    void deleteOneRecord(DynamicDataDefinition dataDefinition, String id);

    /**
     * Update dynamic data definition object one record by json.
     * @param dataDefinition
     * @param id
     * @param dataJson
     * @return
     */
    int updateOneRecord(DynamicDataDefinition dataDefinition, String id, JsonNode dataJson);

    /**
     * Page Query data definition object.
     * @param dataDefinition
     * @param dataJson
     * @param page
     * @return
     */
    PageResult pageFind(DynamicDataDefinition dataDefinition, JsonNode dataJson, Page page);

    /**
     * Batch update data
     * @param dataDefinition
     * @param dataJson
     * @return
     */
    int updateBatch(DynamicDataDefinition dataDefinition, JsonNode dataJson);

    /**
     * Batch insert data
     * @param dataDefinition
     * @param dataJsonList
     * @return
     */
    int insertBatch(DynamicDataDefinition dataDefinition, List<JsonNode> dataJsonList);
}
