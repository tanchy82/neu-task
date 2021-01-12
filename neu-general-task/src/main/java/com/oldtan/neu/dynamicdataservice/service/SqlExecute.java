package com.oldtan.neu.dynamicdataservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
public interface SqlExecute {

    /**
     * Query data definition object record count.
     * @param model
     * @return
     */
    long getCount(SqlDataDefinitionModel model);

    /**
     * Query data definition object column definition json.
     * @param model
     * @return
     */
    Object getColumnDefinition(SqlDataDefinitionModel model);

    /**
     * Insert data definition object one record by json.
     * @param model
     * @param dataJson
     * @return
     */
    void insertData(SqlDataDefinitionModel model, JsonNode dataJson);

    /**
     * Query data definition object one record by id.
     * @param model
     * @param id
     * @return
     */
    Object getOneRecord(SqlDataDefinitionModel model, String id);

    /**
     * Delete data definition object one record by id.
     * @param model
     * @param id
     * @return
     */
    void deleteOneRecord(SqlDataDefinitionModel model, String id);

    /**
     * Update data definition object one record by json.
     * @param model
     * @param id
     * @param dataJson
     * @return
     */
    int updateOneRecord(SqlDataDefinitionModel model, String id, JsonNode dataJson);

    /**
     * Page Query data definition object.
     * @param model
     * @param pageNumber
     * @param pageSize
     * @return
     */
    Object findAll(SqlDataDefinitionModel model, int pageNumber, int pageSize);

}
