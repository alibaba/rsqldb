package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * @author fgm
 * @date 2023/3/21
 * @description 计算设定值上下限比率  getSpHighLowerRatio(lowerLevel,highLevel,spValue,value)
 */

public class SpHighLowerRatioAccumulator extends AggregateFunction<Number, SpHighLowerRatioAccumulator.SpHighLowerRatioAccum> {

    @Override
    public SpHighLowerRatioAccum createAccumulator() {
        return new SpHighLowerRatioAccum();
    }

    /**
     * 计算设定值上下限比率: 监控时间段内，cv_sp_ll(设定值下限) + cv_sp(设定值) < cv_pv(测量值) < cv_sp(设定值) + cv_sp_hl(设定值上限)的次数/cv_pv(测量值)的总次数*100
     *
     * @param highLowerRatioAccum
     * @return
     */
    @Override
    public Number getValue(SpHighLowerRatioAccum highLowerRatioAccum) {
        if (highLowerRatioAccum._totalCount == 0) {
            return 0;
        }
        double ratio = highLowerRatioAccum._inScopeCount * 1.0 / highLowerRatioAccum._totalCount;
        highLowerRatioAccum._ratio = NumberUtil.getDoubleValue(ratio, 4) * 100;
        return highLowerRatioAccum._ratio;
    }

    /**
     * 计算上下限比率  getHighLowerRatio(lowerLevel,highLevel,spValue,value)
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(SpHighLowerRatioAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        if (parameters.length < 4) {
            //log.error("SpHighLowerRatioAccumulator parameters less 4!");
            return;
        }
        Double spLowerLevel = ParameterUtil.getDouble(parameters[0]);
        Double spHighLevel = ParameterUtil.getDouble(parameters[1]);
        Double spValue = ParameterUtil.getDouble(parameters[2]);
        Double value = ParameterUtil.getDouble(parameters[3]);
        if (null == spLowerLevel || null == spHighLevel || null == value || null == value) {
            return;
        }
        Double lowerLevel = spValue + spLowerLevel;
        Double highLevel = spValue + spHighLevel;
        accumulator._totalCount++;
        if (value <= highLevel && value >= lowerLevel) {
            accumulator._inScopeCount++;
        }
    }

    public static class SpHighLowerRatioAccum {
        //总数
        private int _totalCount;
        //范围内总数
        private int _inScopeCount;
        //比率
        private double _ratio;
    }

}
