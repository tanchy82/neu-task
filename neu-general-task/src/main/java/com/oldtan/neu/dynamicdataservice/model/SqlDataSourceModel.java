package com.oldtan.neu.dynamicdataservice.model;

import com.alibaba.druid.pool.DruidDataSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotBlank;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 2020-12-16
 */
@Data
@ToString
public class SqlDataSourceModel {

    @NotBlank
    private String id, dbUrl, dbUsername, dbPassword, driverClassName, dbType;

    @JsonIgnore
    private DruidDataSource dataSource;

}
