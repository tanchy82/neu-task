package com.oldtan.neu.model.repository;

import com.oldtan.neu.model.entity.DynamicDataDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @Description: DynamicDataDefinition Entity Repository
 * @Author: tanchuyue
 * @Date: 21-1-13
 */
@Repository
public interface DynamicDataDefinitionRepository extends JpaRepository<DynamicDataDefinition, String> {

    void deleteAllByDataNameAndAndDatasourceId(String dataName, String dataSourceId);

}
