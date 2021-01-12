package com.oldtan.neu.dynamicdataservice.service.impl;

import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataDefinition;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
@Component
public class SqlDynamicDataDefinitionImpl implements SqlDynamicDataDefinition {

    private Set<SqlDataDefinitionModel> dataDefinitionSet = new ConcurrentSkipListSet();

    @Override
    public Optional<SqlDataDefinitionModel> isExist(SqlDataDefinitionModel definitionModel) {
        Assert.notNull(definitionModel, "definitionModel parameter is not null.");
        return dataDefinitionSet.stream()
                .filter((model) -> model.getSqlDataSourceModel().getUrl().equalsIgnoreCase(definitionModel.getSqlDataSourceModel().getUrl()))
                .filter((model) -> model.getSqlDataSourceModel().getDriverClassName().equalsIgnoreCase(definitionModel.getSqlDataSourceModel().getDriverClassName()))
                .filter((model) -> model.getSqlDataSourceModel().getUsername().equalsIgnoreCase(definitionModel.getSqlDataSourceModel().getUsername()))
                .filter((model) -> model.getName().equalsIgnoreCase(definitionModel.getName()))
                .filter((model) -> model.getId().equalsIgnoreCase(definitionModel.getId()))
                .findAny();
    }

    @Override
    @SneakyThrows
    public SqlDataDefinitionModel create(SqlDataDefinitionModel model) {
        Assert.notNull(model, "SqlDataDefinitionModel parameter is not null.");
        Assert.notNull(model.getSqlDataSourceModel().getDataSource(), "SqlDataSourceModel parameter is not null.");
        dataDefinitionSet.add(model);
        return null;
    }

    @Override
    public void delete(SqlDataDefinitionModel model) {
        Assert.notNull(model, "definitionModel id parameter is not null.");
        dataDefinitionSet.remove(model);
    }

    @Override
    public void deleteByDataSourceId(String dataSourceId){
        Assert.notNull(dataSourceId, "dataSourceId id parameter is not null.");
        dataDefinitionSet.stream().filter((model) -> model.getSqlDataSourceModel().getId().equalsIgnoreCase(dataSourceId))
                .forEach((model -> dataDefinitionSet.remove(model)));
    }

    @Override
    public Optional<SqlDataDefinitionModel> get(String id) {
        return dataDefinitionSet.stream().filter((model) -> model.getId().equalsIgnoreCase(id)).findAny();
    }

    @Override
    public List<SqlDataDefinitionModel> findBySourceId(String dataSourceId){
        Assert.notNull(dataSourceId, "dataSource id parameter is not null.");
        return dataDefinitionSet.stream()
                .filter((source) -> dataSourceId.equalsIgnoreCase(source.getSqlDataSourceModel().getId()))
                .collect(Collectors.toList());
    }

}
