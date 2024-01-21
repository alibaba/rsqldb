package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * @author fgm
 * @date 2023/3/21
 * @description 计算上下限比率  getHighLowerRatio(lowerLevel,highLevel,value)
 */
public class HighLowerRatioAccumulator extends AggregateFunction<Number, HighLowerRatioAccumulator.HighLowerRatioAccum> {

    @Override
    public HighLowerRatioAccum createAccumulator() {
        return new HighLowerRatioAccum();
    }

    /**
     * 计算上下限比率: 监控时间段内，cv_ll(CV下限) < cv_pv(测量值) < cv_hl(CV上限)的次数/cv_pv(测量值)的总次数*100
     *
     * @param highLowerRatioAccum
     * @return
     */
    @Override
    public Number getValue(HighLowerRatioAccum highLowerRatioAccum) {
        if (highLowerRatioAccum._totalCount == 0) {
            return 0;
        }
        double ratio = highLowerRatioAccum._inScopeCount * 1.0 / highLowerRatioAccum._totalCount;
        highLowerRatioAccum._ratio = NumberUtil.getDoubleValue(ratio, 4) * 100;
        return highLowerRatioAccum._ratio;
    }

    /**
     * 计算上下限比率  getHighLowerRatio(lowerLevel,highLevel,value)
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(HighLowerRatioAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        if (parameters.length < 3) {
            return;
        }
        Double lowerLevel = ParameterUtil.getDouble(parameters[0]);
        Double highLevel = ParameterUtil.getDouble(parameters[1]);
        Double value = ParameterUtil.getDouble(parameters[2]);
        if (null == lowerLevel || null == highLevel || null == value) {
            return;
        }
        accumulator._totalCount++;
        if (value <= highLevel && value >= lowerLevel) {
            accumulator._inScopeCount++;
        }
    }

    public static class HighLowerRatioAccum {
        //总数
        private int _totalCount;
        //范围内总数
        private int _inScopeCount;
        //比率
        private double _ratio;
    }

}
