package com.oldtan.neu.dynamicdataservice.service;

import com.oldtan.neu.dynamicdataservice.api.dto.DynamicDatasourceDto;
import com.oldtan.neu.dynamicdataservice.model.SqlDataSourceModel;

import java.sql.SQLException;
import java.util.Collection;

/**
 * @Description: Dynamic DataSource interface
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
public interface SqlDynamicDataSource {

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
    SqlDataSourceModel changeVo(DynamicDatasourceDto datasourceDto);

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

    /**
     * Query all SqlDataSourceModel set or list.
     * @return
     */
    Collection<SqlDataSourceModel> findAll();

}
