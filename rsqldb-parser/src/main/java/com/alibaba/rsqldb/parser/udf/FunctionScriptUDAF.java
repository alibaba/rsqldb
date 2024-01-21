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

import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.streams.script.function.service.IFunctionService;
import org.apache.rocketmq.streams.script.service.udf.UDAFScript;

public class FunctionScriptUDAF extends UDAFScript {
    protected Map<String, Object> fieldName2DefaultValue = new HashMap<>();
    protected String getAccumulateValueFieldName;
    protected String accumulateScriptValue;
    private int paramterSize;

    public FunctionScriptUDAF() {
        this.accumulateMethodName = "accumulate";
        this.createAccumulatorMethodName = "createAccumulator";
        this.getValueMethodName = "getValue";
        this.retractMethodName = "retract";
        this.mergeMethodName = "merge";
        this.methodName = "eval";
        this.initMethodName = "open";
        this.initParameters = new Object[] {};
    }

    /**
     * 在加载时，初始化对象。应该支持，本地class load，从文件load和从远程下载。远程下载部分待测试
     */
    @Override
    protected boolean initBeanClass(IFunctionService iFunctionService) {
        ScriptUDAF scriptUDAF = new ScriptUDAF(this.fieldName2DefaultValue, getAccumulateValueFieldName, accumulateScriptValue, paramterSize);
        this.instance = scriptUDAF;
        initMethod();
        return true;
    }

    @Override
    protected Object[] convert(String... parameters) {
        if (parameters == null) {
            return new Object[0];
        }
        Object[] datas = new Object[parameters.length];
        //第一个参数是内置的
        for (int i = 0; i < parameters.length; i++) {
            datas[i] = accumulateFunctionConfigure.getParameterDataTypes()[i + 1].getData(parameters[i]);
        }
        return datas;
    }

    @Override
    protected Object createMergeParamters(Iterable its) {
        return its;
    }

    public Map<String, Object> getFieldName2DefaultValue() {
        return fieldName2DefaultValue;
    }

    public void setFieldName2DefaultValue(Map<String, Object> fieldName2DefaultValue) {
        this.fieldName2DefaultValue = fieldName2DefaultValue;
    }

    public String getGetAccumulateValueFieldName() {
        return getAccumulateValueFieldName;
    }

    public void setGetAccumulateValueFieldName(String getAccumulateValueFieldName) {
        this.getAccumulateValueFieldName = getAccumulateValueFieldName;
    }

    public String getAccumulateScriptValue() {
        return accumulateScriptValue;
    }

    public void setAccumulateScriptValue(String accumulateScriptValue) {
        this.accumulateScriptValue = accumulateScriptValue;
    }

    public int getParamterSize() {
        return paramterSize;
    }

    public void setParamterSize(int paramterSize) {
        this.paramterSize = paramterSize;
    }
}
