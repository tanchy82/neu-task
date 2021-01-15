package com.oldtan.neu.dynamicdataservice.service;

import javax.sql.DataSource;

/**
 * @Description: Collect data structure interface
 * @Author: tanchuyue
 * @Date: 21-1-15
 */
public interface CollectDataStructure {

    /**
     * Collect data structure by single data source
     * @param dataSource
     */
    void CollectDataStructureBySingleDatasource(DataSource dataSource);

}
