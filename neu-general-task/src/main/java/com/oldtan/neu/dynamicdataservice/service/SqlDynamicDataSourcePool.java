package com.oldtan.neu.dynamicdataservice.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.oldtan.neu.model.entity.DynamicDatasource;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.ToString;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * @Description: Dynamic DataSource interface
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
public interface SqlDynamicDataSourcePool {

    /**
     * Build SqlDynamicDataSourceVO by DynamicDatasource
     * @param dynamicDatasource
     * @return
     */
    SqlDynamicDataSourceVO buildSqlDynamicDataSourceVO(DynamicDatasource dynamicDatasource);

    /**
     * Create sql Data source
     * @param dynamicDataSourceVO
     * @return
     */
    @SneakyThrows
    SqlDynamicDataSourceVO create(SqlDynamicDataSourceVO dynamicDataSourceVO);

    /**
     * Delete SqlDynamicDataSourceVO
     * @param id
     */
    void delete(String id);

    /**
     * Check SqlDynamicDataSourceVO is exist by id
     * @param id
     * @return
     */
    boolean isExist(String id);

    /**
     * Get SqlDynamicDataSourceVO
     * @param id
     * @return
     */
    Optional<SqlDynamicDataSourceVO> get(String id);

    @Data
    @ToString
    @Builder
    class SqlDynamicDataSourceVO implements Serializable {

        private String id, dbUrl, dbUsername, dbPassword, driverClassName, dbType, dbName;

        @JsonIgnore
        private DruidDataSource dataSource;

        @Override
        public boolean equals(Object o) {
            Predicate<SqlDynamicDataSourceVO> predicate = (vo) -> vo.getDbType().equalsIgnoreCase(this.dbType);
            predicate.and((vo) -> vo.getDbUrl().equalsIgnoreCase(this.getDbUrl()));
            predicate.and((vo) -> vo.getDbUsername().equalsIgnoreCase(this.dbUsername));
            if (o instanceof SqlDynamicDataSourceVO){
                return predicate.test((SqlDynamicDataSourceVO)o);
            }
            return false;
        }

    }

}
