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
package com.alibaba.rsqldb.parser.parser.function;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.function.expression.ScriptFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptFunctionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    private Map<String, String> sqlFunctionName2ScriptFunctionName = new HashMap<>();

    public ScriptFunctionParser() {
        sqlFunctionName2ScriptFunctionName.put("BETWEEN ASYMMETRIC", "between");
        sqlFunctionName2ScriptFunctionName.put("NOT BETWEEN ASYMMETRIC", "notbetween");
        sqlFunctionName2ScriptFunctionName.put("IS UNKNOWN", "!null");
        sqlFunctionName2ScriptFunctionName.put("is not null", "!null");
        sqlFunctionName2ScriptFunctionName.put("is null", "null");
        sqlFunctionName2ScriptFunctionName.put("CHAR_LENGTH", "len");
        sqlFunctionName2ScriptFunctionName.put("equal", "equal");
        sqlFunctionName2ScriptFunctionName.put("REGEXP_REPLACE", "REGEXP_REPLACE");
        sqlFunctionName2ScriptFunctionName.put("REGEXP_EXTRACT", "REGEXP_EXTRACT");
        sqlFunctionName2ScriptFunctionName.put("concat", "concat");
        //concat_ws
        sqlFunctionName2ScriptFunctionName.put("not", "!");
        sqlFunctionName2ScriptFunctionName.put("concat_ws", "concat_ws");
        sqlFunctionName2ScriptFunctionName.put("SUBSTRING", "blink_substring");
        sqlFunctionName2ScriptFunctionName.put("substr", "blink_substr");
        sqlFunctionName2ScriptFunctionName.put("abs", "abs");
        sqlFunctionName2ScriptFunctionName.put("md5", "md5");

        sqlFunctionName2ScriptFunctionName.put("window_start", "window_start");
        sqlFunctionName2ScriptFunctionName.put("window_end", "window_end");
        sqlFunctionName2ScriptFunctionName.put("STR_TO_MAP", "STR_TO_MAP");


        sqlFunctionName2ScriptFunctionName.put("UNIX_TIMESTAMP", "unixtime");
        sqlFunctionName2ScriptFunctionName.put("floor", "floor");
        sqlFunctionName2ScriptFunctionName.put("JSON_VALUE", "json_get");
        sqlFunctionName2ScriptFunctionName.put("INSTR", "blink_instr");
        sqlFunctionName2ScriptFunctionName.put("SPLIT_INDEX", "splitindex");
        // sqlFunctionName2ScriptFunctionName.put("TRIM","trim");

        sqlFunctionName2ScriptFunctionName.put("DATEDIFF", "dateDiff");
        sqlFunctionName2ScriptFunctionName.put("DATE_FORMAT", "format");
        sqlFunctionName2ScriptFunctionName.put("from_unixtime", "fromunixtime");
        sqlFunctionName2ScriptFunctionName.put("concat_distinct", "concat_distinct");

        sqlFunctionName2ScriptFunctionName.put("coalesce", "coalesce");
        sqlFunctionName2ScriptFunctionName.put("STRING_SPLIT", "STRING_SPLIT");
        sqlFunctionName2ScriptFunctionName.put("reverse", "reverse");
        sqlFunctionName2ScriptFunctionName.put("IS_DIGIT", "is_number");
        sqlFunctionName2ScriptFunctionName.put("UUID", "uuid");
        sqlFunctionName2ScriptFunctionName.put("PROCTIME","curstamp");


        sqlFunctionName2ScriptFunctionName.put("CHAR", "char");
        sqlFunctionName2ScriptFunctionName.put("UPPER", "upper");
        sqlFunctionName2ScriptFunctionName.put("LPAD", "lpad");
        sqlFunctionName2ScriptFunctionName.put("RPAD", "rpad");
        sqlFunctionName2ScriptFunctionName.put("REPEAT", "repeat");
        sqlFunctionName2ScriptFunctionName.put("REVERSE", "reverse");
        sqlFunctionName2ScriptFunctionName.put("SPLIT_INDEX", "splitindex");
        sqlFunctionName2ScriptFunctionName.put("KEYVALUE", "keyvalue");
        sqlFunctionName2ScriptFunctionName.put("HASH_CODE", "hashcode");
        sqlFunctionName2ScriptFunctionName.put("MD5", "md5");
        sqlFunctionName2ScriptFunctionName.put("INITCAP", "initcap");
        sqlFunctionName2ScriptFunctionName.put("PARSE_URL", "parse_url");
        sqlFunctionName2ScriptFunctionName.put("TO_BASE64", "tobase64");
        sqlFunctionName2ScriptFunctionName.put("FROM_BASE64", "frombase64");

        sqlFunctionName2ScriptFunctionName.put("CONV","conv");
        sqlFunctionName2ScriptFunctionName.put("LOG","log");
        sqlFunctionName2ScriptFunctionName.put("CARDINALITY","cardinality");
        sqlFunctionName2ScriptFunctionName.put("LN","ln");
        sqlFunctionName2ScriptFunctionName.put("PI","pi");
        sqlFunctionName2ScriptFunctionName.put("POWER","power");
        sqlFunctionName2ScriptFunctionName.put("*", "multiplication");
        sqlFunctionName2ScriptFunctionName.put("/", "division");
        sqlFunctionName2ScriptFunctionName.put("+", "addition");
        sqlFunctionName2ScriptFunctionName.put("-", "subtraction");
        sqlFunctionName2ScriptFunctionName.put("COT","cot");
        sqlFunctionName2ScriptFunctionName.put("ABS","abs");
        sqlFunctionName2ScriptFunctionName.put("TAN","tan");
        sqlFunctionName2ScriptFunctionName.put("ASIN","asin");
        sqlFunctionName2ScriptFunctionName.put("ATAN","atan");
        sqlFunctionName2ScriptFunctionName.put("SIN","sin");
        sqlFunctionName2ScriptFunctionName.put("COS","cos");
        sqlFunctionName2ScriptFunctionName.put("BITOR","bitor");
        sqlFunctionName2ScriptFunctionName.put("BITXOR","bitxor");
        sqlFunctionName2ScriptFunctionName.put("BITNOT","bitnot");
        sqlFunctionName2ScriptFunctionName.put("BITAND","bitand");
        sqlFunctionName2ScriptFunctionName.put("LOG10","log10");
        sqlFunctionName2ScriptFunctionName.put("LOG2","log2");
        sqlFunctionName2ScriptFunctionName.put("E","e");
        sqlFunctionName2ScriptFunctionName.put("RAND","rand");
        sqlFunctionName2ScriptFunctionName.put("BIN","bin");
        sqlFunctionName2ScriptFunctionName.put("ROUND","round");
        sqlFunctionName2ScriptFunctionName.put("FLOOR","floor");
        sqlFunctionName2ScriptFunctionName.put("IS_DECIMAL","IS_DECIMAL");
        sqlFunctionName2ScriptFunctionName.put("MOD","mod");


        sqlFunctionName2ScriptFunctionName.put("NOW", "curstamp_second");
        sqlFunctionName2ScriptFunctionName.put("CURRENT_TIMESTAMP","now");
        sqlFunctionName2ScriptFunctionName.put("DATE_FORMAT","dateFormat");
        sqlFunctionName2ScriptFunctionName.put("UNIX_TIMESTAMP","curstamp_second");
        sqlFunctionName2ScriptFunctionName.put("FROM_UNIXTIME","fromunixtime");
        sqlFunctionName2ScriptFunctionName.put("DATEDIFF","dateDiff");
        sqlFunctionName2ScriptFunctionName.put("DATE_SUB","");
        sqlFunctionName2ScriptFunctionName.put("DATE_ADD","dateadd");
        sqlFunctionName2ScriptFunctionName.put("WEEK","weekofyear");
        sqlFunctionName2ScriptFunctionName.put("YEAR","year");
        sqlFunctionName2ScriptFunctionName.put("DATE_FORMAT","format");
//        sqlFunctionName2ScriptFunctionName.put("DATE_FORMAT_TZ","");
        sqlFunctionName2ScriptFunctionName.put("QUARTER","quarter");
        sqlFunctionName2ScriptFunctionName.put("HOUR","hour");
        sqlFunctionName2ScriptFunctionName.put("DAYOFMONTH","day");
        sqlFunctionName2ScriptFunctionName.put("MINUTE","minute");
        sqlFunctionName2ScriptFunctionName.put("MONTH","month");
        sqlFunctionName2ScriptFunctionName.put("TO_TIMESTAMP","timestamp");
//        sqlFunctionName2ScriptFunctionName.put("TO_TIMESTAMP_TZ","");
        sqlFunctionName2ScriptFunctionName.put("TO_DATE","to_date");
//        sqlFunctionName2ScriptFunctionName.put("CONVERT_TZ","");






    }


    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        String name = sqlBasicCall.getOperator().getName().toUpperCase();
        String functionName = getFunctionName(name);
        String scriptValue = functionName + "(";
        // String varName = functionName;
        if (nodeList != null && nodeList.size() > 0) {
            boolean isFirst = true;
            for (SqlNode sqlNode : nodeList) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    scriptValue = scriptValue + ScriptFunction.SPLIT_SIGN;
                }
                IParseResult result = parseSqlNode(tableDescriptor, sqlNode);
                if (result instanceof ScriptParseResult) {
                    ScriptParseResult scriptParseResult = (ScriptParseResult)result;
                    if (!scriptParseResult.getScript().startsWith("(")) {
                        //  tableDescriptor.addScript(scriptParseResult.getScript());
                    }

                }
                scriptValue = scriptValue + result.getValueForSubExpression();
            }
        }

        scriptValue = scriptValue + ")";
        String result = null;

        //生成函数
        scriptValue = scriptValue.replace(ScriptFunction.SPLIT_SIGN, ",");
        result = ParserNameCreator.createName(functionName);
        scriptValue = result + "=" + scriptValue + ";";
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        if (tableDescriptor.isWhereStage()) {
            tableDescriptor.addScript(scriptValue);
            scriptParseResult.setReturnValue(result);
            return scriptParseResult;
        } else {
            scriptParseResult.setReturnValue(result);
            scriptParseResult.addScript(tableDescriptor, scriptValue);
            return scriptParseResult;
        }

    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            String name = sqlBasicCall.getOperator().getName().toUpperCase();
            if (StringUtil.isNotEmpty(getFunctionName(name))) {
                return true;
            }

        }
        return false;
    }

    protected String getFunctionName(String name) {
        String functionName = sqlFunctionName2ScriptFunctionName.get(name);
        if (StringUtil.isNotEmpty(functionName)) {
            return functionName;
        }
        name = name.toLowerCase();
        functionName = sqlFunctionName2ScriptFunctionName.get(name);
        if (StringUtil.isNotEmpty(functionName)) {
            return functionName;
        }

        name = name.toUpperCase();
        functionName = sqlFunctionName2ScriptFunctionName.get(name);
        return functionName;
    }
}
