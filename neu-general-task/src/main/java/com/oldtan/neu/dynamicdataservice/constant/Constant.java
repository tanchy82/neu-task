package com.oldtan.neu.dynamicdataservice.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
public class Constant {

    public final static Map<String, String> SQL_DATABASE_DRIVER_CLASS = new HashMap<>();

    static {
        SQL_DATABASE_DRIVER_CLASS.put("mysql", "com.mysql.cj.jdbc.Driver");
        SQL_DATABASE_DRIVER_CLASS.put("oracle", "com.mysql.cj.jdbc.Driver");
    }

}
