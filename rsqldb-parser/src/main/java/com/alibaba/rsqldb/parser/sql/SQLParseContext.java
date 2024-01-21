package com.alibaba.rsqldb.parser.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SQLParseContext {

    private static final String WINDOW_BUILDER = "window_builder";
    protected static ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<>();

    private SQLParseContext() {
        threadLocal.set(new HashMap<>());
    }

    private static Map<String, Object> get() {
        Map<String, Object> map = threadLocal.get();
        if (map == null) {
            map = new HashMap<>();
            threadLocal.set(map);
        }
        return map;
    }

    private static void remove() {
        threadLocal.remove();
    }

    public static Object getWindowBuilder() {
        Map<String, Object> map = get();
        return map.get(WINDOW_BUILDER);
    }

    public static void setWindowBuilder(Object windowBuilder) {
        Map<String, Object> map = get();
        map.put(WINDOW_BUILDER, windowBuilder);
    }

    public static void removeWindowBuilder() {
        Map<String, Object> map = get();
        map.remove(WINDOW_BUILDER);
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("test1", "value1");

        Properties properties1 = new Properties();
        properties1.putAll(properties);
        System.out.println(properties1.get("test1"));

        properties.put("test1", "value2");
        System.out.println(properties1.get("test1"));
    }
}
