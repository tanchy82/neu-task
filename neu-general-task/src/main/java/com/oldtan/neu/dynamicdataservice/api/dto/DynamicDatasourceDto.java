package com.oldtan.neu.dynamicdataservice.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
@Data
@ApiModel("Dynamic data source dto object.")
public class DynamicDatasourceDto implements Serializable {

    @ApiModelProperty("Dynamic data source primary key id.")
    @NotNull
    private String id;

    @NotNull
    @ApiModelProperty("Data base type.")
    private String dbType;

    @NotNull
    @ApiModelProperty("Data base connect info.")
    private JsonNode dbConnect;


}
