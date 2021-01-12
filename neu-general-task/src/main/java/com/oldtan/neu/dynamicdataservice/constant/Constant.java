package com.oldtan.neu.dynamicdataservice.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
public class Constant {

    public final static Map<String, String> sqlDatabaseDriverClass = new HashMap<>();

    static {
        sqlDatabaseDriverClass.put("mysql", "com.mysql.cj.jdbc.Driver");
        sqlDatabaseDriverClass.put("oracle", "com.mysql.cj.jdbc.Driver");
    }

}
