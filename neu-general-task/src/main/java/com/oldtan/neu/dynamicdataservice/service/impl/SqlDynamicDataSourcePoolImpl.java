package com.oldtan.neu.dynamicdataservice.service.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oldtan.neu.dynamicdataservice.constant.Constant;
import com.oldtan.neu.dynamicdataservice.model.SqlDataSourceModel;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataDefinition;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import com.oldtan.neu.model.entity.DynamicDatasource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
@Component
@Slf4j
public class SqlDynamicDataSourcePoolImpl implements SqlDynamicDataSourcePool {

    private BlockingQueue<SqlDataSourceModel> dataSourceQueue = new ArrayBlockingQueue(10);

    @Autowired
    private SqlDynamicDataDefinition sqlDynamicDataDefinition;

    @Override
    public SqlDataSourceModel changeVo(DynamicDatasource datasourceDto){
        SqlDataSourceModel vo = new ObjectMapper().convertValue(datasourceDto, SqlDataSourceModel.class);
        vo.setDriverClassName(Constant.SQL_DATABASE_DRIVER_CLASS.get(datasourceDto.getDbType()));
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
            druidDataSource.setUrl(dataSourceModel.getDbUrl()); //"jdbc:mysql://124.70.93.116:3306"
            druidDataSource.setUsername(dataSourceModel.getDbUsername());
            druidDataSource.setPassword(dataSourceModel.getDbPassword());
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
                druidDataSource.close();
                throw new RuntimeException(String.format("%s datasource initialization failure.", dataSourceModel.toString()), e);
            }
            return druidDataSource;
        };

        Function<DruidDataSource, SqlDataSourceModel> f = (dds) -> {
            dataSourceModel.setDataSource(dds);
            if (dataSourceQueue.remainingCapacity() < 1)  {
                try {
                    dataSourceQueue.take().getDataSource().close();
                }catch (InterruptedException e){
                    log.warn("Retrieves and removes the head of this dataSourceQueue, but an element becomes available.");
                }
            }
            dataSourceQueue.offer(dataSourceModel) ;
            return dataSourceModel;
        };

        Optional<SqlDataSourceModel> optional = Optional.ofNullable(dataSourceQueue.stream()
                .filter((m) -> m.getDbType().equalsIgnoreCase(dataSourceModel.getDbType()))
                .filter((m) -> m.getDbUrl().equalsIgnoreCase(dataSourceModel.getDbUrl()))
                .filter((m) -> m.getDbUsername().equalsIgnoreCase(dataSourceModel.getDbUsername()))
                .findFirst().orElseGet(() -> f.apply(supplier.get())));
        return optional.get();
    }

    @Override
    public void delete(String id) {
        Assert.notNull(id, "SqlDataSourceModel id is not null.");
        dataSourceQueue.stream().filter((model) -> id.equalsIgnoreCase(model.getId())).findAny()
                .ifPresent((model) -> {
                    model.getDataSource().close();
                    dataSourceQueue.remove(model);
                    sqlDynamicDataDefinition.deleteByDataSourceId(id);
                    log.info(String.format("Delete dataSourceModel id is %s", id));
        });
    }

    @Override
    public boolean isExist(String id){
        return dataSourceQueue.stream().filter((model) -> id.equalsIgnoreCase(model.getId())).findAny().isPresent() ? true : false;
    }

    @Override
    public SqlDataSourceModel get(String id) {
        Assert.notNull(id, "SqlDataSourceModel id is not null.");
        return dataSourceQueue.stream().filter((model) -> id.equalsIgnoreCase(model.getId())).findFirst().get();
    }

}