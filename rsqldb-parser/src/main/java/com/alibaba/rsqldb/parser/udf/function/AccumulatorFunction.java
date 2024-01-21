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
package com.alibaba.rsqldb.parser.udf.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.parser.udf.FunctionScriptUDAF;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class AccumulatorFunction {
    public static String ACCUMULATOR_UDF_KEY = "ACCUMULATOR_UDF_KEY";

    public static void main(String[] args) {
        AccumulatorFunction accumulatorFunction = new AccumulatorFunction();
        List<FieldMeta> list = accumulatorFunction.parseFieldMeta("_count=0,_sum=0,_avg=0,list:list,map:_map");
        System.out.println(list.size());
    }

    @FunctionMethod(value = "accumulator")
    public void accumulator(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldNamesConstant) {
        if (fieldNamesConstant == null) {
            return;
        }
        String fieldNamesStr = FunctionUtils.getConstant(fieldNamesConstant);
        List<FieldMeta> fieldMetas = parseFieldMeta(fieldNamesStr);

        FunctionScriptUDAF functionScriptUDAF = (FunctionScriptUDAF)message.getMessageBody().get(ACCUMULATOR_UDF_KEY);
        if (functionScriptUDAF == null) {
            throw new RuntimeException("function udaf can not support ,need functionScriptUDAF object in message");
        }
        for (FieldMeta fieldMeta : fieldMetas) {
            if (fieldMeta.defaultValue != null) {
                message.getMessageBody().put(fieldMeta.fieldName, fieldMeta.defaultValue);
            }

        }
        functionScriptUDAF.setFieldName2DefaultValue(message.getMessageBody());
    }

    @FunctionMethod(value = "getaccumulatorValue")
    public void getaccumulatorValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName) {
        if (fieldName == null) {
            throw new RuntimeException("function udaf can not support ,need fieldName in function defined");
        }
        FunctionScriptUDAF functionScriptUDAF = (FunctionScriptUDAF)message.getMessageBody().get(ACCUMULATOR_UDF_KEY);
        if (functionScriptUDAF == null) {
            throw new RuntimeException("function udaf can not support ,need functionScriptUDAF object in message");
        }
        functionScriptUDAF.setGetAccumulateValueFieldName(fieldName);
    }

    @FunctionMethod(value = "accumulate")
    public void accumulate(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int parameterSize, String scriptValue) {
        if (scriptValue == null) {
            throw new RuntimeException("function udaf can not support ,need scriptValue in function defined");
        }
        FunctionScriptUDAF functionScriptUDAF = (FunctionScriptUDAF)message.getMessageBody().get(ACCUMULATOR_UDF_KEY);
        functionScriptUDAF.setParamterSize(parameterSize);
        if (functionScriptUDAF == null) {
            throw new RuntimeException("function udaf can not support ,need functionScriptUDAF object in message");
        }
        functionScriptUDAF.setAccumulateScriptValue(FunctionUtils.getConstant(scriptValue));
    }

    @FunctionMethod(value = "getDouble")
    public Double getDoubleValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return Double.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getDouble")
    public Double getDoubleValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        return Double.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getInt")
    public Integer getIntValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return Integer.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getString")
    public String getString(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return (fieldValue.toString());
    }

    @FunctionMethod(value = "getBoolean")
    public Boolean getBoolean(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return Boolean.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getObject")
    public Object getObject(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        return fieldValue;
    }

    @FunctionMethod(value = "getFloat")
    public Float getFloat(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return Float.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getByte")
    public Byte getByte(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return Byte.valueOf(fieldValue.toString());
    }

    @FunctionMethod(value = "getJson")
    public JSONObject getJSON(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") int index) {
        Object fieldValue = message.getMessageBody().getDoubleValue(index + "");
        if (fieldValue == null) {
            return null;
        }
        return (JSONObject)fieldValue;
    }

    @FunctionMethod(value = "getJsonField")
    public Object getJSONField(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") JSONObject jsonObject, String fieldName) {
        return jsonObject.get(fieldName);
    }

    //double:_count=0,double:_sum=0,double:_avg=0,list<string>:list,map<string,string>:_map
    private List<FieldMeta> parseFieldMeta(String str) {

        List<FieldMeta> fieldMetas = new ArrayList<>();

        for (String value : str.split(",")) {
            String[] values = value.split(":");
            FieldMeta fieldMeta = new FieldMeta();
            String[] nameAndDefaultValue = null;
            String typeName = null;
            if (values.length == 2) {
                typeName = values[0];
                fieldMeta.typeName = typeName;
                nameAndDefaultValue = values[1].split("=");
            } else {
                nameAndDefaultValue = values[0].split("=");
            }
            String fieldName = nameAndDefaultValue[0];
            String defaultValue = nameAndDefaultValue.length > 1 ? nameAndDefaultValue[1] : null;
            fieldMeta.defaultValue = defaultValue;
            fieldMeta.fieldName = fieldName;
            if ("list".equals(typeName)) {
                fieldMeta.defaultValue = new ArrayList<>();
            } else if ("set".equals(typeName)) {
                fieldMeta.defaultValue = new HashSet<>();
            } else if ("map".equals(typeName)) {
                fieldMeta.defaultValue = new HashMap<>();
            }
            fieldMetas.add(fieldMeta);
        }
        return fieldMetas;

    }

    protected class FieldMeta {
        String fieldName;
        Object defaultValue;
        String typeName;
    }

}
