package com.oldtan.neu.dynamicdataservice.api;

import com.oldtan.neu.model.entity.DynamicDataDefinition;
import io.swagger.annotations.ApiParam;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description: Publish data service Restful api
 * @Author: tanchuyue
 * @Date: 21-1-15
 */
@RestController
@RequestMapping("/publishDataService")
public class PublishDataServiceRest {

    /**
     *
     * @param dto
     * @return
     */
    @PostMapping
    public @ApiParam Object publish(@Validated @ApiParam DynamicDataDefinition dto){
        /** 1、business logic check */
        /** 2、 */
        return null;
    }

}
