package com.oldtan.neu.model.repository;


import com.oldtan.neu.model.entity.DynamicDatasource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-4
 */
@Repository
public interface DynamicDatasourceRepository extends JpaRepository<DynamicDatasource, String> {

    Optional<DynamicDatasource> findFirstByDbUrlAndDbTypeAndDbUsername(String dbUrl, String dbType, String dbUsername);

}
