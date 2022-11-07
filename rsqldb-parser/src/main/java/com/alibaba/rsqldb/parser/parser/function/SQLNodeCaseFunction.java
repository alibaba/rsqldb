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

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;

public class SQLNodeCaseFunction extends AbstractSelectNodeParser<SqlCase> {

    @Override
    public IParseResult parse(SelectSqlBuilder selectSQLBuilder, SqlCase sqlCase) {
        String resultVarName = ParserNameCreator.createName("case");
        SqlNodeList whenSqlNodes = sqlCase.getWhenOperands();
        SqlNodeList thenSqlNodes = sqlCase.getThenOperands();
        SqlNode elseSqlNode = sqlCase.getElseOperand();
        int i = 0;
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        boolean isFirst = true;
        selectSQLBuilder.getScripts().add("start_if();");
        StringBuilder stringBuilder = new StringBuilder();
        for (SqlNode sqlNode : whenSqlNodes.getList()) {
            // int parseStage=tableDescriptor.getParseStage();
            // tableDescriptor.switchWhere();
            IParseResult whenSqlVar = parseSqlNode(selectSQLBuilder, sqlNode);
            String when = whenSqlVar.getValueForSubExpression();
            // tableDescriptor.setParseStage(parseStage);
            IParseResult thenSqlVar = parseSqlNode(selectSQLBuilder, thenSqlNodes.get(i));
            String then = thenSqlVar.getValueForSubExpression();

            String scriptValue = null;
            if (isFirst) {
                stringBuilder.append("if(" + when + "){" + resultVarName + "=" + then + ";}");
                isFirst = false;
            } else {
                stringBuilder.append("elseif(" + when + "){" + resultVarName + "=" + then + ";}");
            }

            i++;
        }
        String elseValue = parseSqlNode(selectSQLBuilder, elseSqlNode).getValueForSubExpression();
        String scriptValue = "else{" + resultVarName + "=" + elseValue + ";};";
        stringBuilder.append(scriptValue);
        stringBuilder.append("end_if();");
        scriptParseResult.addScript(selectSQLBuilder, stringBuilder.toString());
        scriptParseResult.setReturnValue(resultVarName);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlCase;
    }
}
