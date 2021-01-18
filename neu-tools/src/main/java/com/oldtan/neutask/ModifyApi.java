package com.oldtan.neutask;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-18
 */
@RestController
public class ModifyApi {

    @Autowired
    private ModifyData modifyData;

    @PostMapping("/modify")
    public String test(@Validated @RequestBody Dto dto){
        /*ModifyData data = new ModifyData();
        Map<String, String> map = new HashMap<>();
        map.put("mapping","boolean");
        map.put("name","text");
        data.producer("suzhou_ihbe_dataset_meta-lichen", "ihbe_dataset_meta-lichen", map);*/

        modifyData.modify(dto.index, dto.getName(), dto.getData());
        return "sucess";
    }

    @Data
    @ApiModel("modify data model")
    static class Dto {
        @NotBlank
        @ApiModelProperty("index")
        private String index;
        @NotBlank
        @ApiModelProperty("index data property name field")
        private String name;
        @NotEmpty
        @ApiModelProperty("index data property mapping field data")
        private Map<String, String> data;
    }

}
