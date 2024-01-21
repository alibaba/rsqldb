package com.aliyun.et.industry.stream.domain.dipper.udaf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fgm
 * @date 2023/3/21
 * @description
 */
public class NumberUtil {

    /**
     * 判断数值类型的正则表达式 满足：小数，负数等一系列有效数字 不满足：含有e的数字
     */
    private static final Pattern pattern = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");

    /**
     * 获取四舍五入值
     *
     * @param value 目标值
     * @param scale 保留位数
     * @return
     */
    public static double getDoubleValue(double value, int scale) {
        BigDecimal decimalValue = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return decimalValue.doubleValue();
    }

    /**
     * 获取下限值
     *
     * @param value 目标值
     * @param scale 保留位数
     * @return
     */
    public static double getDoubleValueFloor(double value, int scale) {
        BigDecimal decimalValue = new BigDecimal(value).setScale(scale, RoundingMode.FLOOR);
        return decimalValue.doubleValue();
    }

    /**
     * 将一个字符串转成数值对象，如果转换失败则返回null
     *
     * @param v 待转换的字符串对象
     * @return 对应的数值对象
     * @see NumberFormat#parse(String)
     */
    public static Number getNumberFromString(String v) {
        try {
            return NumberFormat.getInstance().parse(v);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 判读一个字符串是否是数值类型
     *
     * @param value 待检查的字符串对象
     * @return 是否为数值类型
     */
    public static boolean checkNumberFromString(String value) {
        return pattern.matcher(value).matches();
    }

    /**
     * 将一个对象转成数值对象，如果转换失败则返回null
     *
     * @param v 待转换的数值对象
     * @return 对应的数值对象
     */
    public static Number getNumber(Object v) {
        Number vv = null;
        if (v != null) {
            if (v instanceof Number) {
                vv = (Number)v;
            } else if (v instanceof String) {
                vv = getNumberFromString((String)v);
            } else if (v instanceof Character) {
                vv = (Character)v - '0';
            } else if (v instanceof Boolean) {
                vv = 1;
            }
        }
        return vv;
    }

    /**
     * 将一个对象转成数值int
     *
     * @param v 待转换的数值对象
     * @return 对应的数值对象
     */
    public static Integer toInt(Object v) {
        Integer vv = null;
        if (v != null) {
            if (v instanceof Integer) {
                vv = (Integer)v;
            } else if (v instanceof Number) {
                vv = ((Number)v).intValue();
            } else if (v instanceof String) {
                vv = Integer.valueOf((String)v);
            } else if (v instanceof Character) {
                vv = (Character)v - '0';
            }
        }
        return vv;
    }

    /**
     * 解析int类型
     *
     * @param intValue
     * @param defaultValue
     * @return
     */
    public static int parseInt(String intValue, int defaultValue) {
        try {
            if (StringUtils.isEmpty(intValue)) {
                return defaultValue;
            }
            return Integer.valueOf(intValue);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public static void main(String[] args) {
        double value = 3.141592654;
        System.out.println(getDoubleValue(value, 4));
        System.out.println(getDoubleValueFloor(value, 4));

    }
}
