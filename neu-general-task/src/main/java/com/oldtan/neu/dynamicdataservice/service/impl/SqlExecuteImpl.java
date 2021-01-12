package com.oldtan.neu.dynamicdataservice.service.impl;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.Page;
import cn.hutool.db.PageResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.oldtan.neu.dynamicdataservice.model.SqlDataDefinitionModel;
import com.oldtan.neu.dynamicdataservice.service.SqlExecute;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-16
 */
@Component
public class SqlExecuteImpl implements SqlExecute {

    @Override
    @SneakyThrows
    public long getCount(SqlDataDefinitionModel model) {
        Assert.notNull(model, "Parameter SqlDataDefinitionModel is not null.");
        return Db.use(model.getSqlDataSourceModel().getDataSource())
                .queryNumber(String.format("SELECT count(*) from %s", model.getName())).longValue();
    }

    @Override
    @SneakyThrows
    public Object getColumnDefinition(SqlDataDefinitionModel model) {
        Assert.notNull(model, "Parameter SqlDataDefinitionModel is not null.");
        return Db.use(model.getSqlDataSourceModel().getDataSource()).query(String.format("desc %s", model.getName()));
    }

    @Override
    @SneakyThrows
    public void insertData(SqlDataDefinitionModel model, JsonNode dataJson) {
        Entity entity = Entity.create(model.getName());
        dataJson.fieldNames().forEachRemaining((col) -> entity.set(col, dataJson.findValue(col).textValue()));
        Db.use(model.getSqlDataSourceModel().getDataSource()).insert(entity);
    }

    @Override
    @SneakyThrows
    public Object getOneRecord(SqlDataDefinitionModel model, String id) {
        Assert.notNull(model, "Parameter SqlDataDefinitionModel is not null.");
        Assert.notNull(id, "Parameter id is not null.");
        return Db.use(model.getSqlDataSourceModel().getDataSource())
                .queryOne(String.format("SELECT * FROM %s where id = '%s'", model.getName(), id));
    }

    @Override
    @SneakyThrows
    public void deleteOneRecord(SqlDataDefinitionModel model, String id) {
        Assert.notNull(model, "Parameter SqlDataDefinitionModel is not null.");
        Assert.notNull(id, "Parameter id is not null.");
        Db.use(model.getSqlDataSourceModel().getDataSource()).del(model.getName(), "id", id);
    }

    @Override
    @SneakyThrows
    public int updateOneRecord(SqlDataDefinitionModel model, String id, JsonNode dataJson) {
        Entity data = Entity.create();
        dataJson.fieldNames().forEachRemaining((col) -> data.set(col, dataJson.findValue(col).textValue()));
        data.remove("id");
        return Db.use(model.getSqlDataSourceModel().getDataSource()).update(data,
                Entity.create(model.getName()).set("id", id));
    }

    @Override
    @SneakyThrows
    public Object findAll(SqlDataDefinitionModel model, int pageNumber, int pageSize) {
        PageResult<Entity> pageResult = Db.use(model.getSqlDataSourceModel().getDataSource())
                .page(Entity.create(model.getName()), new Page(pageNumber, pageSize > 20 ? 20 : pageSize));
        Map<String, Object> result = new HashMap(8);
        result.put("records", pageResult.listIterator());
        Map<String, Object> pageInfo = new HashMap(8);
        pageInfo.put("page", pageResult.getPage());
        pageInfo.put("pageSize", pageResult.getPageSize());
        pageInfo.put("total", pageResult.getTotal());
        pageInfo.put("totalPage", pageResult.getTotalPage());
        result.put("pageInfo", pageInfo);
        return result;

    }

}
