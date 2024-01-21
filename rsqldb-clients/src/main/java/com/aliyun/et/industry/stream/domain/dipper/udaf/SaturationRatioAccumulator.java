package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author fgm
 * @date 2023/4/13
 * @description 计算上下限饱和率: getSaturationRatio(lowerLevel,highLevel,value)
 */

public class SaturationRatioAccumulator extends AggregateFunction<Number, SaturationRatioAccumulator.SaturationRatioAccum> {

    //误差值
    private static final double epsilon = 1e-4;

    @Override
    public SaturationRatioAccum createAccumulator() {
        return new SaturationRatioAccum();
    }

    /**
     * MV上下限饱和率 监控时间段内，[abs(mv_pv/mv_hl-1)<epsilon + abs(mv_pv/mv_ll-1)<epsilon的次数] / mv_pv的次数 *100 （epsilon=1e-4)
     *
     * @param saturationRatioAccum
     * @return
     */
    @Override
    public Number getValue(SaturationRatioAccum saturationRatioAccum) {
        if (saturationRatioAccum._totalCount == 0) {
            return 0;
        }
        double ratio = saturationRatioAccum._inScopeCount * 1.0 / saturationRatioAccum._totalCount;
        saturationRatioAccum._ratio = NumberUtil.getDoubleValue(ratio, 4) * 100;
        return saturationRatioAccum._ratio;
    }

    /**
     * 计算开放比率 getSaturationRatio(lowerLevel,highLevel,value)
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(SaturationRatioAccumulator.SaturationRatioAccum accumulator, Object... parameters) {
        if (parameters.length < 3) {
            // log.error("SaturationRatioAccumulator parameters less 3!");
            return;
        }
        Double lowerLevel = ParameterUtil.getDouble(parameters[0]);
        Double highLevel = ParameterUtil.getDouble(parameters[1]);
        Double value = ParameterUtil.getDouble(parameters[2]);
        if (null == lowerLevel || null == highLevel || null == value) {
            return;
        }
        accumulator._totalCount++;
        if (Math.abs(value / highLevel - 1) <= epsilon || Math.abs(value / lowerLevel - 1) <= epsilon) {
            accumulator._inScopeCount++;
        }

    }

    public static class SaturationRatioAccum {
        //总数
        private int _totalCount;
        //范围内总数
        private int _inScopeCount;
        //比率
        private double _ratio;
    }

}
