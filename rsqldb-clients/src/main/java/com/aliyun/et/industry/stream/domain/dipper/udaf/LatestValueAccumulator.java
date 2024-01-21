package com.aliyun.et.industry.stream.domain.dipper.udaf;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;

/**
 * @author fgm
 * @date 2023/3/21
 * @description 最新值查询:  getLatestValue('latestKey','${context}') latestKey:    最新值唯一键 ${context}:   上下文
 */
@Function
@UDAFFunction("getLatestValue")
public class LatestValueAccumulator extends AggregateFunction<Object, LatestValueAccumulator.LatestValueAccum> {

    private static final Map<String, Object> LATEST_VALUE_MAP = Maps.newConcurrentMap();

    @Override
    public LatestValueAccum createAccumulator() {
        return new LatestValueAccum();
    }

    @Override
    public Object getValue(LatestValueAccum latestValueAccum) {
        return latestValueAccum._latestValue;
    }

    /**
     * 计算最新值    getLatestValue('latestKey','${context}')
     *
     * @param accumulator
     * @param parameters
     */
    public void accumulate(LatestValueAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        if (parameters.length < 2) {
            return;
        }
        String latestKey = ParameterUtil.getString(parameters[0]);
        if (null == latestKey) {
            return;
        }
        if (null == parameters[1]) {
            return;
        }
        if (!(parameters[1] instanceof JSONObject)) {
            return;
        }
        JSONObject context = (JSONObject)parameters[1];
        String tenantCode = context.getString("tenantCode");
        String gatewayCode = context.getString("gatewayCode");
        String nodeCode = context.getString("nodeCode");
        String measurePoint = context.getString("measurePoint");
        Object value = context.get("value");
        String nodeFullName = ParameterUtil.join("/", tenantCode, gatewayCode, nodeCode);

        String pointFullName = ParameterUtil.join("/", nodeFullName, measurePoint);
        String latestFullName = ParameterUtil.join("/", nodeFullName, latestKey);
        //减少缓存更新
        if (latestFullName.equalsIgnoreCase(pointFullName)) {
            updateLatestValue(latestFullName, value);
        }
        Object latestValue = LATEST_VALUE_MAP.get(latestFullName);
        accumulator._latestFullName = latestFullName;
        accumulator._latestValue = latestValue;
    }

    /**
     * 更新最新值
     *
     * @param latestKey
     * @param value
     */
    private void updateLatestValue(String latestKey, Object value) {
        if (value != null) {
            LATEST_VALUE_MAP.put(latestKey, value);
        }
    }

    public static class LatestValueAccum {
        private String _latestFullName;
        private Object _latestValue;
    }

}
