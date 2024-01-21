package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * @author fgm
 * @date 2023/3/20
 * @description 相对基准变化比例 getBaseRelative(`value`,${baseValue}) value: 当前数据 baseValue: 基准数据
 */

public class BaseRelativeAccumulator extends AggregateFunction<Number, BaseRelativeAccumulator.OpenRateRelativeAccum> {

    private static final double MAX_RATE = 1000000.0;

    @Override
    public OpenRateRelativeAccum createAccumulator() {
        return new OpenRateRelativeAccum();
    }

    @Override
    public Number getValue(OpenRateRelativeAccum baseOpenRateAccum) {
        return baseOpenRateAccum._relativeRate;
    }

    /**
     * 计算基准开放比率 （监控时间段内控制器在线率/基准时间段内控制器在线率 - 1）*100，若基准时间段内控制器在线率为0，输出10^6
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(OpenRateRelativeAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            // log.info("OpenRateRelativeAccumulator parameters is empty!");
            return;
        }
        if (parameters.length < 2) {
            // log.info("OpenRateRelativeAccumulator parameters.length less 2");
            return;
        }
        accumulator.value = ParameterUtil.getDouble(parameters[0]);
        accumulator.baseValue = ParameterUtil.getDouble(parameters[1]);
        if (accumulator.baseValue == null || accumulator.baseValue == 0) {
            accumulator._relativeRate = MAX_RATE;
            return;
        }

        if (accumulator.value == 0) {
            accumulator._relativeRate = 100.0;
            return;
        }
        //相对基准变化率
        double relativeRate = accumulator.value / accumulator.baseValue - 1;
        //保留2位小数 单位:%
        accumulator._relativeRate = NumberUtil.getDoubleValue(relativeRate, 4) * 100;
        if (accumulator._relativeRate > MAX_RATE) {
            accumulator._relativeRate = MAX_RATE;
        }
    }

    public static class OpenRateRelativeAccum {
        //变化率
        private Double value;
        //基准变化率
        private Double baseValue;
        //相对基准变化比例
        private Double _relativeRate;
    }

}
