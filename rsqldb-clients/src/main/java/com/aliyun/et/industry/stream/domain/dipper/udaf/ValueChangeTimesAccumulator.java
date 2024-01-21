package com.aliyun.et.industry.stream.domain.dipper.udaf;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;

/**
 * @author fgm
 * @date 2023/3/21
 * @description 获取值变化次数: getValueChangeTimes(valueKey,`value`)
 */

public class ValueChangeTimesAccumulator extends AggregateFunction<Number, ValueChangeTimesAccumulator.ValueChangeTimesAccum> {

    @Override
    public ValueChangeTimesAccum createAccumulator() {
        return new ValueChangeTimesAccum();
    }

    /**
     * 获取值变化次数
     *
     * @param valueChangeTimesAccum
     * @return
     */
    @Override
    public Number getValue(ValueChangeTimesAccum valueChangeTimesAccum) {
        return valueChangeTimesAccum._count;
    }

    /**
     * 计算值变化次数
     * 监控时间段内，cv_ll和cv_hl发生变化的次数;
     * cv_sp、cv_sp_ll和cv_sp_hl发生变化的次数;
     * ...
     *
     * @param accumulator
     * @param parameters
     */
    public synchronized void accumulate(ValueChangeTimesAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        if (parameters.length < 2) {
            return;
        }
        String valueKey = ParameterUtil.getString(parameters[0]);
        Object currentValue = parameters[1];
        //空值不参与统计
        if (null == valueKey || null == currentValue) {
            return;
        }
        Object oldValue = accumulator._valueMap.get(valueKey);
        //首次记录,不增加变化次数
        if (null == oldValue) {
            //统计值变化次数
            accumulator._valueMap.put(valueKey, currentValue);
            return;
        }
        //有值记录
        if (currentValue.equals(oldValue)) {
            return;
        }
        //统计值变化次数
        //  System.out.println("ValueChangeTimes2 count:"+accumulator._count+",valueKey is:"+valueKey+"oldValue is:"+oldValue+",currentValue is:"+currentValue+",valueMap is :"+accumulator._valueMap+"Object is:"+accumulator);
        accumulator._valueMap.put(valueKey, currentValue);
        accumulator._count++;
        ThreadContext threadContext = ThreadContext.getInstance();
        AbstractContext context = threadContext.get();
        System.out.println(context.get("start_time") + "-" + context.get("end_time") + "-" + DateUtil.getCurrentTimeString() + "----ValueChangeTimes2 count:" + accumulator._count + ",valueKey is:" + valueKey + ",valueMap is :" + accumulator._valueMap + "Object is:" + accumulator);
    }

    public static class ValueChangeTimesAccum {
        private Map<String, Object> _valueMap = Maps.newConcurrentMap();
        private int _count;

    }

}
