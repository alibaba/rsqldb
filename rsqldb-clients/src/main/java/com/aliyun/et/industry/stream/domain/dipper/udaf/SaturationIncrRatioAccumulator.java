package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author fgm
 * @date 2023/4/13
 * @description 计算增量上下限饱和率 getSaturationIncrRatio(lowerLevel,highLevel,value)
 */
public class SaturationIncrRatioAccumulator extends AggregateFunction<Number, SaturationIncrRatioAccumulator.SaturationIncrRatioAccum> {
    //误差值
    private static final double epsilon = 1e-4;

    @Override
    public SaturationIncrRatioAccum createAccumulator() {
        return new SaturationIncrRatioAccum();
    }

    /**
     * 监控时间段内: abs{[mv_pv(i)-mv_pv(i-1)]/mv_delta_hl-1}<epsilon 和 abs{[mv_pv(i)-mv_pv(i-1)]/mv_delta_ll-1}<epsilon 的次数/ mv_pv的次数*100 其中 epsilon=1e-4
     *
     * @param saturationIncrRatioAccum
     * @return
     */
    @Override
    public Number getValue(SaturationIncrRatioAccum saturationIncrRatioAccum) {
        if (saturationIncrRatioAccum._totalCount == 0) {
            return 0;
        }
        double ratio = saturationIncrRatioAccum._inScopeCount * 1.0 / saturationIncrRatioAccum._totalCount;
        saturationIncrRatioAccum._ratio = NumberUtil.getDoubleValue(ratio, 4) * 100;
        return saturationIncrRatioAccum._ratio;
    }

    public void accumulate(SaturationIncrRatioAccum accumulator, Object... parameters) {
        if (parameters.length < 3) {
            //log.error("SaturationIncrRatioAccum parameters less 3!");
            return;
        }
        Double lowerLevel = ParameterUtil.getDouble(parameters[0]);
        Double highLevel = ParameterUtil.getDouble(parameters[1]);
        Double currentValue = ParameterUtil.getDouble(parameters[2]);
        if (null == lowerLevel || null == highLevel || null == currentValue) {
            return;
        }
        accumulator._totalCount++;
        Double lastValue = accumulator._lastValue;
        if (null == lastValue) {
            accumulator._lastValue = currentValue;
            return;
        }
        if (Math.abs((currentValue - lastValue) / highLevel - 1) <= epsilon || Math.abs((currentValue - lastValue) / lowerLevel - 1) <= epsilon) {
            accumulator._inScopeCount++;
        }
        accumulator._lastValue = currentValue;
    }

    public static class SaturationIncrRatioAccum {
        //上次值
        private Double _lastValue;
        //总数
        private int _totalCount;
        //范围内总数
        private int _inScopeCount;
        //比率
        private double _ratio;
    }

}
