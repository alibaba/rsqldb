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
package com.alibaba.rsqldb.parser.parser.builder;

import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinConditionSQLBuilder extends SelectSQLBuilder {
    protected Set<String> dimFieldNames;
    protected String dimAsAlias;
    protected List<String> dimScript = new ArrayList<>();
    protected Map<String, String> newFieldName2Scripts = new HashMap<>();

    public JoinConditionSQLBuilder(Set<String> dimFieldNames, String dimAsAlias) {
        this.dimFieldNames = dimFieldNames;
        this.dimAsAlias = dimAsAlias;
    }

    @Override
    public void addScript(String scriptValue) {

        if (isDimScript(scriptValue)) {
            addDimScript(scriptValue);
        } else {
            super.addScript(scriptValue);
        }

    }

    @Override
    public String getFieldName(String fieldName) {
        int startIndex = fieldName.indexOf(".");
        String aliasName = null;
        if (startIndex != -1) {
            aliasName = fieldName.substring(0, startIndex);
            fieldName = fieldName.substring(startIndex + 1);

        }
        if (dimFieldNames.contains(fieldName) && (aliasName == null || (aliasName != null && dimAsAlias.equals(aliasName)))) {
            return dimAsAlias + "." + fieldName;
        } else {
            return fieldName;
        }
    }

    /**
     * 从script中，把别名去除掉,并加入dim对应掉script list中
     *
     * @param scriptValue
     */
    protected void addDimScript(String scriptValue) {
        FunctionScript functionScript = new FunctionScript(scriptValue);
        functionScript.init();
        List<IScriptExpression> expressions = functionScript
            .getScriptExpressions();
        for (IScriptExpression expression : expressions) {
            if (expression instanceof ScriptExpression) {
                ScriptExpression scriptExpression = (ScriptExpression)expression;
                if (scriptExpression.getNewFieldName() != null) {
                    newFieldName2Scripts.put(scriptExpression.getNewFieldName(), scriptValue);
                }
                List<IScriptParamter> parameters = scriptExpression
                    .getParameters();
                List<IScriptParamter> paramterList = new ArrayList<>();
                paramterList.addAll(parameters);
                if (parameters == null || parameters.size() == 0) {
                    continue;
                }
                int index = 0;
                for (IScriptParamter tmp : parameters) {
                    if (ScriptParameter.class.isInstance(tmp)) {
                        ScriptParameter scriptParamter = (ScriptParameter)tmp;
                        String parameterStr = scriptParamter.getScriptParameterStr();
                        String parameter = getFieldName(parameterStr);
                        if (parameter.startsWith(dimAsAlias + ".")) {
                            int startIndex = parameter.indexOf(".");
                            parameter = parameter.substring(startIndex + 1);
                            scriptParamter = new ScriptParameter(parameter);
                            parameters.set(index, scriptParamter);
                        }
                    }
                    index++;

                }

            }
            dimScript.add(expression.toString());
        }
    }

    protected boolean isDimScript(String scriptValue) {
        for (String fieldName : dimFieldNames) {
            String name = dimAsAlias + "." + fieldName;
            if (scriptValue.contains(name)) {
                return true;
            }
        }
        for (String fieldName : newFieldName2Scripts.keySet()) {
            if (scriptValue.contains(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public String getScriptValue() {
        return createScript(getScripts());
    }

    public String getDimScriptValue() {
        return createScript(dimScript);
    }

    public Set<String> getDimFieldNames() {
        return dimFieldNames;
    }

    public void setDimFieldNames(Set<String> dimFieldNames) {
        this.dimFieldNames = dimFieldNames;
    }

    public String getDimAsAlias() {
        return dimAsAlias;
    }

    public void setDimAsAlias(String dimAsAlias) {
        this.dimAsAlias = dimAsAlias;
    }

    public boolean containsFieldName(String fieldName) {
        return newFieldName2Scripts.containsKey(fieldName);
    }
}
