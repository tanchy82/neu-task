package com.oldtan.neu.dynamicdataservice.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.oldtan.neu.dynamicdataservice.api.dto.DynamicDatasourceDto;
import com.oldtan.neu.dynamicdataservice.constant.Constant;
import com.oldtan.neu.dynamicdataservice.model.SqlDataSourceModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataDefinition;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
@Component
@Slf4j
public class SqlDynamicDataSourceImpl implements SqlDynamicDataSource {

    private Set<SqlDataSourceModel> dataSourceModelSet = new ConcurrentSkipListSet<>();

    @Autowired
    private SqlDynamicDataDefinition sqlDynamicDataDefinition;

    @Override
    public SqlDataSourceModel changeVo(DynamicDatasourceDto datasourceDto){
        SqlDataSourceModel vo = BeanUtil.copyProperties(datasourceDto.getDbConnect(), SqlDataSourceModel.class);
        vo.setDatabaseType(datasourceDto.getDbType());
        vo.setDriverClassName(Constant.sqlDatabaseDriverClass.get(datasourceDto.getDbType()));
        return vo;
    }

    @Override
    @SneakyThrows
    public SqlDataSourceModel create(SqlDataSourceModel dataSourceModel){
        Assert.notNull(dataSourceModel, "Sql Data SourceModel parameter is not null.");
        Supplier<DruidDataSource> supplier = () -> {
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setName(dataSourceModel.getId());
            druidDataSource.setDriverClassName(dataSourceModel.getDriverClassName());//com.mysql.cj.jdbc.Driver
            druidDataSource.setUrl(dataSourceModel.getUrl()); //"jdbc:mysql://124.70.93.116:3306"
            druidDataSource.setUsername(dataSourceModel.getUsername());
            druidDataSource.setPassword(dataSourceModel.getPassword());
            druidDataSource.setInitialSize(1);
            druidDataSource.setMaxActive(2);
            druidDataSource.setMaxWait(60000);
            druidDataSource.setMinIdle(1);
            druidDataSource.setTestOnBorrow(true);
            druidDataSource.setTestWhileIdle(true);
            druidDataSource.setValidationQuery("select 1 from dual");
            druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
            druidDataSource.setMinEvictableIdleTimeMillis(180000);
            druidDataSource.setKeepAlive(true);
            druidDataSource.setRemoveAbandoned(true);
            druidDataSource.setRemoveAbandonedTimeout(3600);
            druidDataSource.setLogAbandoned(true);
            try {
                druidDataSource.init();
                log.info(String.format("%s datasource initialization success.", dataSourceModel.toString()));
            } catch (SQLException e) {
                log.error(String.format("%s datasource initialization failure.", dataSourceModel.toString()), e);
                druidDataSource.close();
                throw new RuntimeException(e);
            }
            return druidDataSource;
        };

        Optional<SqlDataSourceModel> optional = Optional.ofNullable(dataSourceModelSet.stream()
                .filter((m) -> m.getDriverClassName().equalsIgnoreCase(dataSourceModel.getDriverClassName()))
                .filter((m) -> m.getUsername().equalsIgnoreCase(dataSourceModel.getUsername()))
                .filter((m) -> m.getUrl().equalsIgnoreCase(dataSourceModel.getUrl()))
                .findFirst().orElseGet(() -> {
                    Optional.of(supplier.get()).ifPresent((source) -> {
                        dataSourceModel.setDataSource(source);
                        dataSourceModelSet.add(dataSourceModel);
                    });
                    return dataSourceModel;
        }));
        return optional.get();
    }

    @Override
    public void delete(String id) {
        Assert.notNull(id, "SqlDataSourceModel id is not null.");
        dataSourceModelSet.stream().filter((model) -> id.equalsIgnoreCase(model.getId())).findAny()
                .ifPresent((model) -> {
                    model.getDataSource().close();
                    dataSourceModelSet.remove(model);
                    sqlDynamicDataDefinition.deleteByDataSourceId(id);
                    log.info(String.format("Delete dataSourceModel id is %s", id));
        });
    }

    @Override
    public boolean isExist(String id){
        return dataSourceModelSet.stream()
                .filter((model) -> id.equalsIgnoreCase(model.getId())).findAny().isPresent() ? true : false;
    }

    @Override
    public SqlDataSourceModel get(String id) {
        Assert.notNull(id, "SqlDataSourceModel id is not null.");
        return dataSourceModelSet.stream().filter((model) -> id.equalsIgnoreCase(model.getId())).findFirst().get();
    }

    @Override
    public Collection<SqlDataSourceModel> findAll(){
        return dataSourceModelSet;
    }

}