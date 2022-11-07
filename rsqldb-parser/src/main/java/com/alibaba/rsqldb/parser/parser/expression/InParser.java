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

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.ConstantParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.rocketmq.streams.common.datatype.StringDataType;

public class InParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> sqlNodeList = sqlBasicCall.getOperandList();
        String functionName = "in";
        if (sqlBasicCall.getOperator().getName().toUpperCase().equals("NOT IN")) {
            functionName = "!in";
        }
        String varName = parseSqlNode(tableDescriptor, sqlNodeList.get(0)).getReturnValue();
        SqlNodeList nodes = (SqlNodeList) sqlNodeList.get(1);
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (SqlNode sqlNode : nodes.getList()) {
            String value = parseSqlNode(tableDescriptor, sqlNode).getValueForSubExpression();
            value = value.replace("'", "\\'");
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }
            stringBuilder.append(value);
        }
        if (tableDescriptor.isSelectStage()) {
            functionName = "contain";
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("NOT IN")) {
                functionName = "!contain";
            }
            String script = stringBuilder.toString();
            script = script.replace("\\'", "'");
            String returnName = ParserNameCreator.createName(functionName);
            script = returnName + "=" + functionName + "(" + varName + "," + script + ");";
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            scriptParseResult.setReturnValue(returnName);
            scriptParseResult.addScript(tableDescriptor, script);
            return scriptParseResult;
        }
        return createExpression(varName, functionName,
            new ConstantParseResult(stringBuilder.toString(), new StringDataType()));
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("IN")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("NOT IN")) {
                return true;
            }
        }
        return false;
    }
}
