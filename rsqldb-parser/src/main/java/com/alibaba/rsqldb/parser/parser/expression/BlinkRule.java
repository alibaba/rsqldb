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
package com.alibaba.rsqldb.parser.parser.expression;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BlinkRule {
    /**
     * rule id
     */
    protected int ruleId;
    /**
     * key: varName
     * value: expression str list
     */
    protected Map<String, List<String>> var2ExpressionList=new HashMap<>();

    public BlinkRule(JSONObject ruleJson, int ruleId) {
        this.ruleId=ruleId;
        Iterator iterator = ruleJson.keySet().iterator();
        while (iterator.hasNext()) {
            String varName = (String) iterator.next();
            String ruleValue = ruleJson.getString(varName);
            String expression = null;
            if (ruleValue.startsWith("$") && ruleValue.endsWith("$")) {
                String regex = ruleValue.substring(1, ruleValue.length() - 1);
                regex=regex.replace("'","\'");
                expression = "(" + varName + ",regex,'" + regex + "')";
            } else if (ruleValue.startsWith("[") && ruleValue.endsWith("]")) {
                long start;
                long end;
                try {
                    start = Long.valueOf(ruleValue.substring(1, ruleValue.indexOf(",")));
                    end = Long.valueOf(ruleValue.substring(ruleValue.indexOf(",") + 1, ruleValue.length() - 1));
                } catch (Exception var11) {
                    start = 1L;
                    end = 0L;
                }
                expression = "((" + varName + ",>=," + start + ")&(" + varName + ",<=," + end + "))";
            } else {
                expression = "(" + varName + ",=,'" + ruleValue + "')";
            }
            List<String> expressions = var2ExpressionList.get(varName);
            if (expressions == null) {
                expressions = new ArrayList<>();
                var2ExpressionList.put(varName, expressions);
            }
            expressions.add(expression);
        }
    }
    public String createExpression(JSONObject msg){
        List<String> expressions=new ArrayList<>();
        for(String var:var2ExpressionList.keySet()){
            if(!msg.containsKey(var)){
                return null;
            }
            String expression=createExpression(var);
            expressions.add(expression);
        }
        if(expressions.size()==0){
            return null;
        }
        return MapKeyUtil.createKey("&",expressions);
    }
    public String createExpression(String varName){
        List<String> expressions=this.var2ExpressionList.get(varName);
        if(expressions==null){
            return null;
        }
        if(expressions.size()==1){
            return expressions.get(0);
        }
        return "("+ MapKeyUtil.createKey("&",expressions) +")";

    }

    public Collection<String> getAllVars() {
        return var2ExpressionList.keySet();
    }

    public int getRuleId() {
        return ruleId;
    }
}
