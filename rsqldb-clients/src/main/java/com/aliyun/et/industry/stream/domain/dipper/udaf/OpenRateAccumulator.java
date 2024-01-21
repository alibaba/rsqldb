package com.aliyun.et.industry.stream.domain.dipper.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;

/**
 * @author fgm
 * @date 2023/3/20
 * @description 计算开放比率:  getOpenRate(`value`)
 */
@Function
@UDAFFunction("getOpenRate")
public class OpenRateAccumulator extends AggregateFunction<Number, OpenRateAccumulator.OpenRateAccum> {

    @Override
    public OpenRateAccum createAccumulator() {
        return new OpenRateAccum();
    }

    /**
     * 计算开放比率: 监控时间段内，mpc_rate为1或true的次数/mpc_rate总次数*100
     *
     * @param openRateAccum
     * @return
     */
    @Override
    public Number getValue(OpenRateAccum openRateAccum) {
        double rate = (openRateAccum.openCount * 1.0) / openRateAccum.totalCount;
        //保留2位小数 单位:%
        openRateAccum._openRate = NumberUtil.getDoubleValue(rate, 4) * 100;
        return openRateAccum._openRate;
    }

    /**
     * 计算开放比率
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(OpenRateAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            boolean isOpen = getIsOpen(parameters);
            accumulator.totalCount++;
            if (isOpen) {
                accumulator.openCount++;
            }
        } catch (Exception e) {
            // log.error("OpenRateAccumulator value is [{}}],ex:{}", parameters[0], ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    private boolean getIsOpen(Object[] parameters) {
        try {
            String inputData = null;
            Object parameter0 = parameters[0];
            if (parameters[0] instanceof String) {
                inputData = (String)parameter0;
            } else {
                inputData = String.valueOf(parameter0);
            }
            if ("1".equals(inputData) || "true".equalsIgnoreCase(inputData)) {
                return true;
            }
            return false;
        } catch (Exception ex) {
            //log.error("getIsOpen ex:{}",ExceptionUtils.getStackTrace(ex));
            return false;
        }

    }

    public static class OpenRateAccum {
        private int openCount;
        private int totalCount;
        private double _openRate;
    }

}
