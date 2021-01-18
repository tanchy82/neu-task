package com.oldtan.neutask;

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
public class TestApi {

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
    static class Dto {
        @NotBlank
        private String index;
        @NotBlank
        private String name;
        @NotEmpty
        private Map<String, String> data;
    }

}
