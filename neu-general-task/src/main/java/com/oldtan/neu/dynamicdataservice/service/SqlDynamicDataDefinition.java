package com.oldtan.neu.dynamicdataservice.service;

import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;

import java.util.List;
import java.util.Optional;

/**
 * @Description: Dynamic Data Definition Interface
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
public interface SqlDynamicDataDefinition {

    /**
     * check Data Definition Model is exist.
     * @param definitionModel
     * @return
     */
    Optional<SqlDataDefinitionModel> isExist(SqlDataDefinitionModel definitionModel);

    /**
     * Create data definition.
     * @param model
     * @return
     */
    SqlDataDefinitionModel create(SqlDataDefinitionModel model);

    /**
     * Delete data definition.
     * @param model
     */
    void delete(SqlDataDefinitionModel model);

    /**
     * Delete data definition.
     * @param dataSourceId
     */
    void deleteByDataSourceId(String dataSourceId);

    /**
     * Get data definition.
     * @param id
     * @return
     */
    Optional<SqlDataDefinitionModel> get(String id);

    /**
     * Get more data definition by data source id.
     * @param dataSourceId
     * @return
     */
    List<SqlDataDefinitionModel> findBySourceId(String dataSourceId);

}
