package com.oldtan.neu.dynamicdataservice.service.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.oldtan.neu.dynamicdataservice.service.SqlDynamicDataSourcePool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @Description: Dynamic DataSource operating handler
 * @Author: tanchuyue
 * @Date: 20-12-11
 */
@Component
@Slf4j
public class SqlDynamicDataSourcePoolImpl implements SqlDynamicDataSourcePool {

    private final BlockingQueue<SqlDynamicDataSourceVO> dataSourceQueue = new ArrayBlockingQueue(10);

    private ReentrantLock lock = new ReentrantLock();

    @Override
    public SqlDynamicDataSourceVO create(final SqlDynamicDataSourceVO dynamicDataSourceVO) {
        Assert.notNull(dynamicDataSourceVO, "This argument is required, it must not be null.");
        /** 1、check is exist */
        Optional<SqlDynamicDataSourceVO> optional =
                dataSourceQueue.stream().filter((v) -> v.equals(dynamicDataSourceVO)).findAny();
        /** 2、supplier DruidDataSource */
        Supplier<DruidDataSource> supplier = () -> {
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setName(dynamicDataSourceVO.getId());
            druidDataSource.setDriverClassName(dynamicDataSourceVO.getDriverClassName());//com.mysql.cj.jdbc.Driver
            druidDataSource.setUrl(dynamicDataSourceVO.getDbUrl()); //"jdbc:mysql://124.70.93.116:3306"
            druidDataSource.setUsername(dynamicDataSourceVO.getDbUsername());
            druidDataSource.setPassword(dynamicDataSourceVO.getDbPassword());
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
                log.info(String.format("%s datasource initialization success.", dynamicDataSourceVO.toString()));
            } catch (SQLException e) {
                druidDataSource.close();
                throw new RuntimeException(String.format("%s datasource initialization failure.", dynamicDataSourceVO.toString()), e);
            }
            return druidDataSource;
        };
        /** 3、DruidDataSource queue operating */
        Consumer<BlockingQueue<SqlDynamicDataSourceVO>> consumer = (dataSourceQueue) -> {
            Stream.of(dataSourceQueue).filter((queue) -> queue.remainingCapacity() < 1).findFirst()
                    .ifPresent((queue) -> {
                        try {
                            dataSourceQueue.take().getDataSource().close();
                        }catch (InterruptedException e){
                            log.warn("Retrieves and removes the head of this dataSourceQueue, but an element becomes available.");
                        }
                    });
            dataSourceQueue.offer(dynamicDataSourceVO) ;
        };
        return optional.orElseGet(() -> {
            lock.lock();
            try {
                return optional.orElseGet(() -> {
                    dynamicDataSourceVO.setDataSource(supplier.get());
                    consumer.accept(dataSourceQueue);
                    return dynamicDataSourceVO;
                });
            } finally {
                lock.unlock();
            } });
    }

    @Override
    public void delete(String id) {
        Assert.notNull(id, "This argument is required, it must not be null.");
        dataSourceQueue.stream().filter((vo) -> id.equalsIgnoreCase(vo.getId())).findAny()
                .ifPresent((vo) -> {
                    vo.getDataSource().close();
                    dataSourceQueue.remove(vo);
                    log.info(String.format("Remove in the queue SqlDynamicDataSourceVO, id is %s ", id));
        });
    }

    @Override
    public boolean isExist(String id){
        return get(id).isPresent();
    }

    @Override
    public Optional<SqlDynamicDataSourceVO> get(String id) {
        Assert.notNull(id, "This argument is required, it must not be null.");
        return dataSourceQueue.stream().filter((vo) -> id.equalsIgnoreCase(vo.getId())).findAny();
    }

}