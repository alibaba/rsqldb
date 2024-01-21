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
package com.alibaba.rsqldb.parser.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class JoinConditionSqlBuilder extends SelectSqlBuilder {
    protected Set<String> dimFieldNames;
    protected String dimAsAlias;
    protected List<String> dimScript = new ArrayList<>();
    protected Map<String, String> newFieldName2Scripts = new HashMap<>();

    protected CreateSqlBuilder builder;

    public JoinConditionSqlBuilder(CreateSqlBuilder builder, Set<String> dimFieldNames, String dimAsAlias) {
        this.dimFieldNames = dimFieldNames;
        this.dimAsAlias = dimAsAlias;
        this.builder = builder;
    }

    public String parseConditionSQL(String condition) {
        String expressionStr = condition;
        if (StringUtil.isEmpty(expressionStr)) {
            return null;
        }
        Map<String, String> flags = new HashMap<>();
        expressionStr = ContantsUtil.doConstantReplace(expressionStr, flags, 10000);

        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", expressionStr, expressions, relationExpressions);
        Map<String, Expression> map = new HashMap<>();
        String aliasName = this.dimAsAlias;
        if (StringUtil.isNotEmpty(aliasName)) {
            aliasName = aliasName + ".";
        }
        for (Expression express : expressions) {
            map.put(express.getName(), express);
            String varName = express.getVarName();
            Object valueObject = express.getValue();
            //如果value不是字符串，不做处理
            if (!express.getDataType().matchClass(String.class)) {
                continue;
            }

            String value = (String)valueObject;
            if (StringUtil.isNotEmpty(aliasName) && value.startsWith(aliasName)) {
                value = value.replace(aliasName, "");

            }
            if (dimFieldNames.contains(value)) {
                express.setValue(value);
                if (varName.indexOf(".") != -1) {
                    varName = varName.split("\\.")[1];
                    express.setVarName(varName);
                }
                continue;
            }

            if (containsFieldName(value)) {
                express.setValue(value);
                if (varName.indexOf(".") != -1) {
                    varName = varName.split("\\.")[1];
                    express.setVarName(varName);
                }
                continue;
            }

            /***
             *
             */
            if (StringUtil.isNotEmpty(aliasName) && varName.startsWith(aliasName)) {
                varName = varName.replace(aliasName, "");

            }
            if (dimFieldNames.contains(varName)) {
                if (value.indexOf(".") != -1) {
                    value = varName.split("\\.")[1];
                }
                express.setVarName(value);
                express.setValue(varName);
                continue;
            }
            if (containsFieldName(varName)) {
                if (value.indexOf(".") != -1) {
                    value = varName.split("\\.")[1];
                }
                express.setVarName(value);
                express.setValue(varName);
                continue;
            }
            throw new RuntimeException("can not parser expression " + expressionStr);

        }
        for (Expression relation : relationExpressions) {
            map.put(relation.getName(), relation);
        }
        if (flags != null && flags.size() > 0) {
            Iterator<Map.Entry<String, String>> it = flags.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                String varName = entry.getKey();
                String value = entry.getValue();
                addScript(varName + "=" + value + ";");
            }
        }
        return expression.toExpressionString(map);
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
        return fieldName;
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

    public CreateSqlBuilder getBuilder() {
        return builder;
    }
}
