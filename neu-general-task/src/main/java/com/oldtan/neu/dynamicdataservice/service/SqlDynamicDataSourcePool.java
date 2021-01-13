package com.oldtan.neu.dynamicdataservice.service;

import com.oldtan.neu.dynamicdataservice.model.SqlDataSourceModel;
import com.oldtan.neu.model.entity.DynamicDatasource;

import java.sql.SQLException;

/**
 * @Description: Dynamic DataSource interface
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
public interface SqlDynamicDataSourcePool {

    /**
     * Create sql Data source
     * @param dataSourceModel
     * @return
     * @throws SQLException
     */
    SqlDataSourceModel create(SqlDataSourceModel dataSourceModel)throws SQLException;

    /**
     * Change datasourceDto to SqlDataSourceModel
     * @param datasourceDto
     * @return
     */
    SqlDataSourceModel changeVo(DynamicDatasource datasourceDto);

    /**
     * Delete Data source
     * @param id
     */
    void delete(String id);

    /**
     * Check data source is exist
     * @param id
     * @return
     */
    boolean isExist(String id);

    /**
     * Get sql data source
     * @param id
     * @return
     */
    SqlDataSourceModel get(String id);

}
