package com.alibaba.rsqldb.parser.util;

import com.alibaba.rsqldb.parser.parser.builder.CreateSQLBuilder;
import org.apache.flink.sql.parser.ddl.SqlWatermark;

import java.util.Map;
import java.util.Set;

public class ThreadLocalUtil {
    public static final ThreadLocal<Map<String, SqlWatermark>> watermarkHolder = new ThreadLocal<>();

    public static final ThreadLocal<Map<String, CreateSQLBuilder>> createSqlHolder = new ThreadLocal<>();

    public static final ThreadLocal<Map<String, Set<String>>> stringSet = new ThreadLocal<>();
}
