package com.oldtan.neu.model.repository;


import com.oldtan.neu.model.entity.DynamicDatasource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-4
 */
@Repository
public interface DynamicDatasourceRepository extends JpaRepository<DynamicDatasource, String> {
}
