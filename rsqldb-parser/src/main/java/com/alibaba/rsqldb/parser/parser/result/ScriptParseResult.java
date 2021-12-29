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
package com.alibaba.rsqldb.parser.parser.result;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 每个解析的返回结果。需要考虑变量，常量和脚本的情况
 */
public class ScriptParseResult implements IParseResult {

    /**
     * 保存3种值，常量，变量的值以及脚本如果有需要返回的变量，也保存在这个字段
     */
    protected String returnValue;

    /**
     * 如果是脚本，则放到这里面
     */
    protected List<String> scriptValueList = new ArrayList<>();

    /**
     * 主要是针对规则引擎规则，确定用到的字段
     */
    protected String varName;

    /**
     * 如果获取的值是为了做子表达式子，会去掉最后的分号
     *
     * @return
     */
    @Override
    public String getValueForSubExpression() {
        if (StringUtil.isNotEmpty(returnValue)) {
            return returnValue;
        }

        String script = getScript();
        if (script.endsWith(";")) {
            script = script.substring(0, script.length() - 1);
        }
        return script;
    }

    @Override
    public String getReturnValue() {
        return returnValue;
    }

    @Override
    public Object getResultValue() {
        return getScript();
    }

    @Override
    public boolean isConstantString() {
        return false;
    }

    public void addScript(String script) {
        this.scriptValueList.add(script);
    }

    public void addScript(SelectSQLBuilder tableDescriptor, String script) {
        tableDescriptor.addScript(script);
        addScript(script);
    }

    /**
     * 获取脚本
     *
     * @return
     */
    public String getScript() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        if (scriptValueList != null && scriptValueList.size() > 0) {
            for (String script : scriptValueList) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    if (!script.trim().endsWith(";")) {
                        stringBuilder.append(";");
                    }
                }
                stringBuilder.append(script);
            }

        }
        String result = stringBuilder.toString();
        if (!result.trim().endsWith(";")) {
            result += ";";
        }

        return result;
    }

    public List<String> getScriptValueList() {
        return scriptValueList;
    }

    public void setScriptValueList(List<String> scriptValueList) {
        this.scriptValueList = scriptValueList;
    }

    public void setReturnValue(String returnValue) {
        this.returnValue = returnValue;
    }
}
