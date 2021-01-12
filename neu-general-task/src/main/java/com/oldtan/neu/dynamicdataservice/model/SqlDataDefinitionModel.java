package com.oldtan.neu.dynamicdataservice.model;

import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 2020-12-16
 */
@Data
@ToString
public class SqlDataDefinitionModel {

    @NotBlank
    private String id, name;

    @NotNull
    private SqlDataSourceModel sqlDataSourceModel;

}
