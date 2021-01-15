package com.oldtan.neu.model.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * @Description: Dynamic data definition entity
 * @Author: tanchuyue
 * @Date: 21-1-13
 */
@Entity
@Data
@JsonIgnoreProperties(value={"hibernateLazyInitializer","handler","fieldHandler"})
@ApiModel(value = "Dynamic data definition entity.")
@NoArgsConstructor
public class DynamicDataDefinition {

    @Id
    @ApiModelProperty("Primary key id.")
    private String id;

    @ApiModelProperty("Association dynamic data source id.")
    private String datasourceId;

    @NotNull
    @ApiModelProperty("Data source link url.")
    private String dbUrl;

    @NotNull
    @ApiModelProperty("Data source link data base name.")
    private String dbName;

    @NotNull
    @ApiModelProperty("Data source link username.")
    private String dbUsername;

    @NotNull
    @ApiModelProperty("Data source link password.")
    private String dbPassword;

    @NotNull
    @ApiModelProperty("Data source link table name.")
    private String dbTableName;

    @NotNull
    @ApiModelProperty("Data source link table type, is table or view.")
    private String dbTableType;

    @NotNull
    @ApiModelProperty("Data resource publish context path.")
    private String publishContext;

    @NotNull
    @ApiModelProperty("Data resource publish type (eg: GET、POST、PUT、DELETE). Support multiple, comma partition.")
    private String publishType;

    @ApiModelProperty("Data resource table schema.")
    @Column(name = "text")
    private String dbTableSchema;

    @JsonIgnore
    private LocalDateTime createTime;

    @JsonIgnore
    private LocalDateTime updateTime;

}
