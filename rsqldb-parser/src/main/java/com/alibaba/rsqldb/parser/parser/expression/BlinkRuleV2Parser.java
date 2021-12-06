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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * create by udf
 */
public class BlinkRuleV2Parser extends AbstractSelectNodeParser<SqlBasicCall> {
    protected String className;
    protected Map<String, List<BlinkRule>> tmpWhiteListOnline = new HashMap();
    protected Map<String, List<BlinkRule>> tmpWhiteListOb = new HashMap();
    protected String functionName;
    public BlinkRuleV2Parser(){}
    public BlinkRuleV2Parser(String fullClassName,String functionName){
        this.className=fullClassName;
        this.functionName=functionName;
        parserExpressions();
    }
    @Override public IParseResult parse(SelectSQLBuilder builder, SqlBasicCall sqlBasicCall) {
        List<SqlNode> sqlNodeList=sqlBasicCall.getOperandList();
        Map<String, List<BlinkRule>> current=tmpWhiteListOnline;

        String modId=sqlNodeList.get(0).toString();
        String isOnline=sqlNodeList.get(1).toString();
        if("1".equals(isOnline)){
            current=this.tmpWhiteListOb;
        }
        List<BlinkRule> blinkRules=current.get(modId);
        String[] kv=new String[sqlNodeList.size()-2];
        for(int i=2;i<sqlNodeList.size();i++){
           kv[i-2]=sqlNodeList.get(i).toString();
        }
        JSONObject msg=createMsg(kv);
        Set<String> varNames=new HashSet<>();

        ScriptParseResult scriptParseResult = new ScriptParseResult();
        boolean isFirst = true;
        if(SelectSQLBuilder.class.isInstance(builder)){
            SelectSQLBuilder sqlBuilder=(SelectSQLBuilder)builder;
            sqlBuilder.getScripts().add("start_if();");
        }
        String resultVarName = ParserNameCreator.createName(functionName);
        StringBuilder stringBuilder = new StringBuilder();
        for(BlinkRule blinkRule:blinkRules){
            String when=blinkRule.createExpression(msg);
            if(when!=null){
                varNames.addAll(blinkRule.getAllVars());
                if (isFirst) {
                    stringBuilder.append("if(" + when + "){" + resultVarName + "=" + blinkRule.getRuleId() + ";}");
                    isFirst = false;
                } else {
                    stringBuilder.append("elseif(" + when + "){" + resultVarName + "=" + blinkRule.getRuleId() + ";}");
                }
            }
        }

        stringBuilder.append(";end_if();");
        scriptParseResult.addScript((SelectSQLBuilder) builder, stringBuilder.toString());
        scriptParseResult.setReturnValue(resultVarName);
        return scriptParseResult;
    }

    protected JSONObject createMsg(String[] kv) {
        JSONObject msg = null;
        if ( kv.length % 2 == 0) {
            msg=new JSONObject();
            for (int i = 0; i < kv.length; i += 2) {
                msg.put(FunctionUtils.getConstant(kv[i]), kv[i + 1]);
            }
        }
        return msg;
    }

    public void parserExpressions(){
        InputStream inputStream = ReflectUtil.forClass(className).getResourceAsStream("/data_4_sas_black_rule_v2.json");
        List<String> rules= FileUtil.loadFileLine(inputStream);
        String line =rules.get(0);
        JSONArray allRules = JSON.parseArray(line);
        Iterator iterator = allRules.iterator();
        while (iterator.hasNext()){
            Object object = iterator.next();
            JSONObject rule_json = JSON.parseObject(object.toString());
            int status = rule_json.getInteger("status");
            int ruleId = rule_json.getInteger("id");
            String  modelId = rule_json.getString("model_id");
            String ruleValue = rule_json.getString("content");
            JSONObject ruleJson=JSONObject.parseObject(ruleValue);
            BlinkRule blinkRule=new BlinkRule(ruleJson,ruleId);
            if (status == 0) {
                if (!tmpWhiteListOnline.containsKey(modelId)) {
                    tmpWhiteListOnline.put(modelId, new ArrayList());
                }

                ((List)tmpWhiteListOnline.get(modelId)).add(blinkRule);
            }

            if (!tmpWhiteListOb.containsKey(modelId)) {
                tmpWhiteListOb.put(modelId, new ArrayList());
            }
            ((List)tmpWhiteListOb.get(modelId)).add(blinkRule);
        }
    }

    /**
     * not scan directly, find it by udf
     * @param sqlNode
     * @return
     */
    @Override public boolean support(Object sqlNode) {
        return false;
    }

}
