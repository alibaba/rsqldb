package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * @author fgm
 * @date 2023/5/10
 * @description 均值计算函数  average(`value`,scale) value: 当前数据 scale: 精度值,可为空，默认为2
 */

public class AverageAccumulator extends AggregateFunction<Number, AverageAccumulator.AverageAccum> {

    @Override
    public AverageAccum createAccumulator() {
        return new AverageAccum();
    }

    @Override
    public Number getValue(AverageAccum averageAccum) {
        return averageAccum._avg;
    }

    /**
     * 计算平均值
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(AverageAccumulator.AverageAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            //log.info("AverageAccumulator parameters is empty!");
            return;
        }
        if (parameters.length < 1) {
            //log.info("AverageAccumulator parameters.length less 1");
            return;
        }
        Double value = ParameterUtil.getDouble(parameters[0]);
        if (null == value) {
            //log.info("AverageAccumulator value is null");
            return;
        }
        int scale = getScale(parameters);
        accumulator._count++;
        accumulator._sum = accumulator._sum + value;
        double average = accumulator._sum / accumulator._count;
        accumulator._avg = NumberUtil.getDoubleValue(average, scale);
    }

    /**
     * 获取精度
     *
     * @param parameters
     * @return
     */
    private int getScale(Object[] parameters) {
        //默认
        if (parameters.length < 2) {
            return 2;
        }
        Integer scale = ParameterUtil.getInteger(parameters[1]);
        return scale;
    }

    public static class AverageAccum {
        private double _count;
        private double _sum;
        private double _avg;
    }

}
