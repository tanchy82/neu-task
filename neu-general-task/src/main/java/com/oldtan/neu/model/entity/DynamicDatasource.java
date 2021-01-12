package com.oldtan.neu.model.entity;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-30
 */
@Entity
@Data
@JsonIgnoreProperties(value={"hibernateLazyInitializer","handler","fieldHandler"})
@ApiModel(value = "Dynamic data source entity.")
public class DynamicDatasource implements Serializable {

    @Id
    @NotNull
    @ApiModelProperty("Primary key id.")
    private String id;

    @NotNull
    @ApiModelProperty("Data base type.")
    private String dbType;

    @NotNull
    @ApiModelProperty("Data base connect info.")
    private String dbConnect;

    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone="GMT+8")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @ApiModelProperty("Data source entity create time.")
    private LocalDateTime createTime;

}
