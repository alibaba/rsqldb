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
package com.alibaba.rsqldb.parser.expression;

import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSelectNodeParser;

import org.apache.calcite.sql.SqlBasicCall;

public class AndRelationParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        String relation = sqlBasicCall.getOperator().getName().equalsIgnoreCase("and") ? "&" : "|";
        String expressionLeft = parseSqlNode(tableDescriptor, sqlBasicCall.getOperandList().get(0))
            .getValueForSubExpression();
        String expressionRight = parseSqlNode(tableDescriptor, sqlBasicCall.getOperandList().get(1))
            .getValueForSubExpression();
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        String scriptValue = "";
        if (isExpression(expressionLeft)) {
            scriptValue = expressionLeft + relation + expressionRight;
            scriptParseResult.addScript("(" + scriptValue + ")");
        } else {
            scriptParseResult.addScript("(" + expressionLeft + relation + expressionRight + ")");
        }
        return scriptParseResult;
    }

    private boolean isExpression(String expressionLeft) {
        return (expressionLeft.startsWith("(") && expressionLeft.endsWith(")"));
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("and")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("or")) {
                return true;
            }
        }
        return false;
    }

}
