/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.parser.udf;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.function.aggregation.CountAccumulator;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;

public class ScriptUDAF {

    protected Map<String, Object> fieldName2DefaultValue = new HashMap<>();
    protected String getAccumulateValueFieldName;
    protected String accumulateScriptValue;
    protected FunctionScript functionScript;
    protected int parameterSize;

    public ScriptUDAF(Map<String, Object> fieldName2DefaultValue, String getAccumulateValueFieldName, String accumulateScriptValue, int parameterSize) {
        this.fieldName2DefaultValue = fieldName2DefaultValue;
        this.getAccumulateValueFieldName = getAccumulateValueFieldName;
        this.accumulateScriptValue = accumulateScriptValue;
        this.parameterSize = parameterSize;
        this.functionScript = new FunctionScript(accumulateScriptValue);
        this.functionScript.init();
    }

    public static void main(String[] args) {
        Method[] methods = CountAccumulator.class.getDeclaredMethods();
        System.out.println(methods.length);
    }

    public CommonUDAF createAccumulator() {
        CommonUDAF commonUDAF = new CommonUDAF();
        commonUDAF.value.putAll(fieldName2DefaultValue);
        return commonUDAF;
    }

    public Object getValue(CommonUDAF accumulator) {
        return accumulator.value.get(getAccumulateValueFieldName);
    }

    public void accumulate(CommonUDAF accumulator, Object... parameters) {
        if (parameterSize > 0 && parameters.length != parameterSize) {
            return;
        }
        JSONObject msg = new JSONObject();
        int index = 0;
        for (Object object : parameters) {
            msg.put(index + "", object);
            index++;
        }
        msg.putAll(accumulator.value);
        Message message = new Message(msg);
        Context context = new Context(message);
        this.functionScript.executeScriptAsFunction(message, context);
        for (String key : fieldName2DefaultValue.keySet()) {
            Object value = msg.get(key);
            if (value != null) {
                accumulator.value.put(key, value);
            }
        }
    }

    public void merge(CommonUDAF accumulator, Iterable<CommonUDAF> its) {

    }

    public void retract(CommonUDAF accumulator, String... parameters) {

    }

    public static class CommonUDAF {
        private JSONObject value = new JSONObject();

    }

}
