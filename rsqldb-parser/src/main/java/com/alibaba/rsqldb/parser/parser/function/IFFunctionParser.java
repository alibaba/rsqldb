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

import java.util.List;

public class IFFunctionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder selectSQLBuilder, SqlBasicCall sqlBasicCall) {
        selectSQLBuilder.getScripts().add("start_if();");
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        SqlNode expression = nodeList.get(0);
        SqlNode success = nodeList.get(1);
        SqlNode failure = nodeList.get(2);
        String expressionFunction = parseSqlNode(selectSQLBuilder, expression).getValueForSubExpression();

        String successValue = parseSqlNode(selectSQLBuilder, success).getValueForSubExpression();
        String failureValue = parseSqlNode(selectSQLBuilder, failure).getValueForSubExpression();
        String resultName = ParserNameCreator.createName("if", selectSQLBuilder.getTableName());
        String scriptValue = "if(" + expressionFunction + "){" + resultName + "=" + successValue + "}else{" + resultName
            + "=" + failureValue + "};";
        scriptValue+="end_if();";
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        scriptParseResult.setReturnValue(resultName);
        scriptParseResult.addScript(selectSQLBuilder, scriptValue);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("if")) {
                return true;
            }
        }
        return false;
    }
}
