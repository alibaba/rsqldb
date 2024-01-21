package com.aliyun.et.industry.stream.domain.dipper.udaf;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fgm
 * @date 2023/3/21
 * @description 参数工具
 */
public class ParameterUtil {

    /**
     * 校验通过
     *
     * @param condition 校验条件
     * @param message   异常消息
     */
    public static void check(boolean condition, String message) {
        if (condition) {
            throw new RuntimeException(message);
        }
    }

    /**
     * 获取String value
     *
     * @param value
     * @return
     */
    public static String getString(Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof String) {
            return (String)value;
        }
        return value.toString();
    }

    /**
     * 获取 Boolean值
     *
     * @param value
     * @return
     */
    public static boolean getBoolean(Object value) {
        if (null == value) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean)value;
        }
        if (value instanceof String) {
            return stringToBoolean((String)value);
        }
        try {
            return Boolean.valueOf(value.toString());
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * String 转 Boolean
     *
     * @param value
     * @return
     */
    private static boolean stringToBoolean(String value) {
        if (StringUtils.isEmpty(value)) {
            return false;
        }
        if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
            return true;
        }
        return false;
    }

    /**
     * 获取按照指定分隔符连接的字符串
     *
     * @param join
     * @param values
     * @return
     */
    public static String getString(String join, Object... values) {
        return Arrays.stream(values).map(value -> getString(value)).collect(Collectors.joining(join));
    }

    /**
     * 获取double value
     *
     * @param value
     * @return
     */
    public static Double getDouble(Object value) {
        if (null == value) {
            return null;
        }
        try {
            if (value instanceof Double) {
                return (Double)value;
            }
            return Double.valueOf(value.toString());
        } catch (Exception ex) {
            // log.error("getDouble ex:{}", ExceptionUtils.getStackTrace(ex));
            return null;
        }
    }

    public static Integer getInteger(Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer)value;
        }
        try {
            return Integer.valueOf(value.toString());
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * 字符串拼接
     *
     * @param split
     * @param datas
     * @return
     */
    public static String join(String split, String... datas) {
        return Arrays.stream(datas).collect(Collectors.joining(split));
    }
}
