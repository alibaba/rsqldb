package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * @author fgm
 * @date 2023/2/8
 * @description 方差计算函数 getVariance(`value`)
 */
public class VarianceAccumulator extends AggregateFunction<Number, VarianceAccumulator.VarianceAccum> {

    @Override
    public VarianceAccum createAccumulator() {
        return new VarianceAccum();
    }

    /**
     * 获取方差
     *
     * @param accumulator
     * @return
     */
    @Override
    public Number getValue(VarianceAccum accumulator) {
        return accumulator.populationVariance;
    }

    public void accumulate(VarianceAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            Number parameter = null;
            Object parameter0 = parameters[0];
            if (parameters[0] instanceof String) {
                parameter = Double.valueOf((String)parameter0);
            } else {
                parameter = (Number)parameter0;
            }
            double newValue = parameter.doubleValue();
            accumulator.count++;
            double meanDifferential = (newValue - accumulator._mean) / accumulator.count;
            double newMean = accumulator._mean + meanDifferential;
            //方差增长
            double dSquaredIncrement = (newValue - newMean) * (newValue - accumulator._mean);
            //新方差
            double newDSquared = accumulator._dSquared + dSquaredIncrement;

            accumulator._mean = newMean;
            accumulator._dSquared = newDSquared;
            //总体方差
            accumulator.populationVariance = NumberUtil.getDoubleValue(accumulator._dSquared / accumulator.count, 2);
            if (accumulator.count > 1) {
                //样本方差
                accumulator.sampleVariance = accumulator.count > 1 ? NumberUtil.getDoubleValue(accumulator._dSquared / (accumulator.count - 1), 2) : 0;
            }

        } catch (Exception e) {
            // log.error("VarianceAccumulator value is [{}}],ex:{}", parameters[0], ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    public static class VarianceAccum {
        private int count;
        private double _mean;
        private double _dSquared;
        //样本方差
        private double sampleVariance;
        //总体方差
        private double populationVariance;
    }

}
