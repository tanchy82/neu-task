package com.oldtan.neutask;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.enums.CellExtraTypeEnum;
import com.alibaba.excel.event.AnalysisEventListener;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-19
 */
@Slf4j
public class ReadExcel {

    public static volatile Map<String, Set<String>> setMap = new ConcurrentHashMap<>();

    public static volatile BlockingQueue<Map<String, Set<String>>> tableQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args){
        String fileName = "/home/tanchy/文档/demo.xlsx";
        EasyExcel.read(fileName, ExcelData.class, new ExcelDataListener())
                .extraRead(CellExtraTypeEnum.MERGE).sheet().doRead();
    }

    @Data
    public static class ExcelData{

        private String table;

        private String column;

    }

    @Slf4j
    public static class ExcelDataListener extends AnalysisEventListener<ExcelData>{

        private volatile String tableName = null;

        @Override
        @SneakyThrows
        public void invoke(ExcelData excelData, AnalysisContext analysisContext) {
            if (tableName == null && excelData.getTable() == null){
                throw new RuntimeException("table name and excel data value first col is not both null!!");
            }else if(excelData.getTable() != null){
                tableName = excelData.getTable();
            }else if(tableName != null){
                excelData.setTable(tableName);
            }
            setMap.computeIfAbsent(excelData.getTable(), (s) -> new HashSet<String>()).add(excelData.getColumn());
        }

        @Override
        public void doAfterAllAnalysed(AnalysisContext analysisContext) {
            setMap.keySet().stream().forEach((s) -> {
                log.info(String.format("table name value is %s , %s", s, setMap.get(s)));
                Map<String, Set<String>> table = new HashMap<>();
                table.put(s, setMap.get(s));
                tableQueue.offer(table);
            });
        }
    }

}
